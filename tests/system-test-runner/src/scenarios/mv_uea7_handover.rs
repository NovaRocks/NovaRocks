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

use super::mv_uea7::{mv_properties, mv_public_error};
use crate::actors::sql::TrinoValue;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use crate::scenarios::mv_uea7::{
    ManagedMvRestFixture, assert_rows, connect, execute, property, require_property,
    require_status_phase, require_three_backends, select_catalog_and_namespace, status,
};
use anyhow::{Context, Result, bail};
use novarocks_cluster_harness::TestClusterOwner;
use std::path::Path;
use std::sync::Mutex;

const CATALOG: &str = "uea7_mv_handover";
const TAKING_DEPLOYMENT: &str = "uea7-taking-deployment";

#[derive(Default)]
pub(super) struct MvOwnerHandover {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvOwnerHandover {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "mv_uea7_handover",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "mv/owner-handover"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let (rest, launch) = ManagedMvRestFixture::start(scenario_root, CATALOG)?;
        let mut fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV handover fixture lock poisoned"))?;
        if fixture.is_some() {
            bail!("managed MV handover fixture was initialized more than once");
        }
        *fixture = Some(rest);
        Ok(launch)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let create_catalog_sql = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV handover fixture lock poisoned"))?
            .as_ref()
            .context("managed MV handover fixture is missing after cluster launch")?
            .create_catalog_sql()
            .to_owned();
        let mut conn = connect(context)?;
        execute(
            context,
            &mut conn,
            "create private REST catalog",
            &create_catalog_sql,
        )?;
        execute(
            context,
            &mut conn,
            "create managed MV namespace",
            "CREATE DATABASE uea7_mv_handover.ns",
        )?;
        execute(
            context,
            &mut conn,
            "create row-lineage source table",
            "CREATE TABLE uea7_mv_handover.ns.orders (k1 INT, v2 BIGINT) \
             TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")",
        )?;
        execute(
            context,
            &mut conn,
            "insert source rows",
            "INSERT INTO uea7_mv_handover.ns.orders VALUES (1, 10), (2, 20)",
        )?;
        select_catalog_and_namespace(context, &mut conn, CATALOG)?;
        execute(
            context,
            &mut conn,
            "create document-managed MV",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 1 \
             REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') \
             AS SELECT k1, SUM(v2) AS total FROM orders GROUP BY k1",
        )?;
        execute(
            context,
            &mut conn,
            "publish initial MV snapshot",
            "REFRESH MATERIALIZED VIEW orders_mv WITH SYNC MODE",
        )?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, total FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "read MV before handover",
        )?;
        require_status_phase(
            context,
            &mut conn,
            CATALOG,
            "orders_mv",
            "MANAGEABLE",
            "verify the original deployment manages the MV",
        )?;

        let before = status(context, &mut conn, CATALOG, "orders_mv")?;
        require_property(&before, "HandoverAvailable", "YES")?;
        let original_owner = property(&before, "LocalOwner")?;
        if original_owner == TAKING_DEPLOYMENT {
            bail!("the fixture's taking deployment matches its current owner");
        }
        let challenge = property(&before, "Challenge")?;
        let handover: Vec<(String, Option<String>)> = mv_properties(
            &conn
                .execute(format!(
                    "CALL novarocks_mv_set_owner('{CATALOG}', 'ns', 'orders_mv', \
                 '{challenge}', '{TAKING_DEPLOYMENT}')"
                ))
                .context("hand over the exact MV through public management SQL")?,
        )?;
        require_property(&handover, "Result", "HANDED_OVER")?;
        require_property(&handover, "NewOwner", TAKING_DEPLOYMENT)?;
        require_property(&handover, "PreviousOwner", original_owner)?;
        context.action("committed a one-use challenge-gated target owner update");
        drop(conn);

        let deadline = context.deadline();
        context.action("restart old deployment against the handed-over lake target");
        context
            .handle()
            .restart_fe_until(deadline)
            .context("restart old deployment after MV handover")?;
        let mut conn = connect(context)?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, total FROM uea7_mv_handover.ns.orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "read handed-over MV from the old deployment",
        )?;
        select_catalog_and_namespace(context, &mut conn, CATALOG)?;
        context.action("verify the handed-over MV is listed as read-only");
        let listed = conn
            .execute(format!("SELECT name, manageability, manageability_reason FROM system.runtime.materialized_view_states WHERE catalog_name = '{CATALOG}' AND schema_name = 'ns'"))
            .context("list handed-over MV after old deployment restart")?;
        let named = |name: &str| -> Result<usize> {
            let columns = listed
                .columns
                .iter()
                .enumerate()
                .filter(|(_, column)| column.name == name)
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            anyhow::ensure!(
                listed.is_result_set() && columns.len() == 1,
                "MV state table omitted or duplicated {name}"
            );
            anyhow::ensure!(
                listed.columns[columns[0]].type_signature.raw_type == "varchar",
                "MV state {name} is not declared text"
            );
            Ok(columns[0])
        };
        let name = named("name")?;
        let manageability_column = named("manageability")?;
        let target = listed.rows.iter().filter(|row| matches!(row.get(name), Some(TrinoValue::Text(value)) if value == "orders_mv")).collect::<Vec<_>>();
        anyhow::ensure!(
            target.len() == 1,
            "handed-over MV is missing or duplicated in the state table"
        );
        let Some(TrinoValue::Text(manageability)) = target[0].get(manageability_column) else {
            bail!("handed-over MV omitted its text manageability")
        };
        if manageability != "READ_ONLY" {
            bail!(
                "handed-over MV was listed as {manageability:?}, expected READ_ONLY; {}",
                context.diagnostics()
            );
        }
        let reason_column = named("manageability_reason")?;
        anyhow::ensure!(
            matches!(target[0].get(reason_column),Some(TrinoValue::Text(reason)) if !reason.is_empty()),
            "read-only handed-over MV omitted its management reason"
        );
        context.action("verify the old deployment cannot publish after handover");
        let error = conn
            .execute("REFRESH MATERIALIZED VIEW orders_mv WITH SYNC MODE")
            .expect_err("old deployment must not manage a handed-over MV");
        let message = mv_public_error(&error)?.message.clone();
        if !message.contains("another deployment")
            && !message.contains("not owned by this process")
            && !message.contains("fresh Current observation")
        {
            bail!(
                "old deployment refresh returned an unexpected error {message:?}; {}",
                context.diagnostics()
            );
        }
        context.action("verified old deployment retains reads and refuses a new publication");
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV handover fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}
