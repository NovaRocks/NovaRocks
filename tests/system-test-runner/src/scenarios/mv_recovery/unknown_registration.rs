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

use super::{
    MvContentEffect, MvNameGraceRestart, assert_content_refresh_explain,
    assert_rest_snapshot_state_unchanged, assert_rows, connect, execute, load_metadata,
    prepare_content_effect_source, property, refresh,
    require_nonaggregate_management_after_restart, require_three_backends, restart_frontend,
    select_catalog_and_database, status,
};
use crate::actors::sql::SqlSession;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::publication_catalog::{
    FixtureControl, FixtureFaultEvidence, PublicationCatalogFixture, PublicationFault,
    RetainedMvCommit,
};
use serde_json::{Value, json};
use std::{fs, path::Path};

const CATALOG: &str = "system_mv_name_grace";
const TARGET: &str = "orders_mv";
const MANAGEMENT_SLOT: &str = "novarocks.doc.v2.slot.novarocks.storage~management";
const RELEASE_PREFIX: &str = "novarocks.doc.v2.mark.novarocks.mv~pending-released~";

#[derive(Clone, Copy)]
pub(super) enum RegistrationEffect {
    PendingReleased,
    ManagementSlot,
}

pub(super) struct MvUnknownRegistration {
    fixture: MvNameGraceRestart,
    effect: RegistrationEffect,
}

impl MvUnknownRegistration {
    pub(super) fn new(effect: RegistrationEffect) -> Self {
        Self {
            fixture: MvNameGraceRestart::default(),
            effect,
        }
    }
}

impl Scenario for MvUnknownRegistration {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "mv_recovery",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        match self.effect {
            RegistrationEffect::PendingReleased => "mv/content-unknown-pending-release",
            RegistrationEffect::ManagementSlot => "mv/content-unknown-management-slot",
        }
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        self.fixture.launch_config(scenario_root)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let (create_catalog, rest_uri, proxy_uri, control) = {
            let fixture = self
                .fixture
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("MV fixture lock poisoned"))?;
            let (rest, proxy) = fixture.as_ref().context("managed MV fixture is missing")?;
            (
                rest.create_catalog_sql(),
                rest.rest_uri().to_owned(),
                proxy.uri().to_owned(),
                proxy.control()?,
            )
        };
        let mut conn = connect(context)?;
        match self.effect {
            RegistrationEffect::PendingReleased => {
                // Separate controls own the definite publication refusal
                // and the subsequent marker's lost success response.
                let mut upstream = PublicationCatalogFixture::start(proxy_uri)?;
                let result = (|| {
                    prepare_content_effect_source(
                        context,
                        &mut conn,
                        CATALOG,
                        &create_catalog.replace(&rest_uri, upstream.uri()),
                    )?;
                    lose_pending_release(
                        context,
                        &mut conn,
                        &rest_uri,
                        &control,
                        &upstream.control()?,
                    )
                })();
                super::control_plan::with_cleanup(result, upstream.shutdown())
            }
            RegistrationEffect::ManagementSlot => {
                prepare_content_effect_source(
                    context,
                    &mut conn,
                    CATALOG,
                    &create_catalog.replace(&rest_uri, &proxy_uri),
                )?;
                lose_incarnation_registration(context, conn, &rest_uri, &control)
            }
        }
    }

    fn teardown(&self) -> Result<()> {
        self.fixture.teardown()
    }
}

fn lose_pending_release(
    context: &mut ScenarioContext,
    conn: &mut SqlSession,
    rest_uri: &str,
    downstream: &FixtureControl,
    upstream: &FixtureControl,
) -> Result<()> {
    let original = load_metadata(context, rest_uri, "ns", TARGET)?;
    let refusal = downstream.fault_mv_commit(
        "ns",
        TARGET,
        RetainedMvCommit::Publication,
        PublicationFault::BeforeDispatchForbidden,
    )?;
    let lost = upstream.fault_mv_commit(
        "ns",
        TARGET,
        RetainedMvCommit::PendingReleased,
        PublicationFault::AfterCommitBeforeResponse,
    )?;
    context.action("lose the release-marker response after an exact known publication refusal");
    let result = conn.execute(MvContentEffect::REFRESH);
    let refusal = refusal.finish()?;
    let lost = lost.finish()?;
    let refused = result.expect_err("the original forbidden publication unexpectedly succeeded");
    let public = super::mv_public_error(&refused)?;
    ensure!(
        refusal.summary().contains("forbidden-not-dispatched")
            && !refusal.summary().contains("request-forwarded"),
        "the refused publication reached the backing catalog: {}",
        refusal.summary()
    );
    let observed = load_metadata(context, rest_uri, "ns", TARGET)?;
    assert_rest_snapshot_state_unchanged(&original, &observed)?;
    ensure!(
        observed["properties"]
            .as_object()
            .is_some_and(|properties| {
                properties.keys().any(|key| key.starts_with(RELEASE_PREFIX))
            }),
        "the successful release did not write its original keyed marker"
    );
    record_lost_effect(
        context,
        lost,
        json!({
            "publication_refusal": refusal.trace(),
            "public_error": public.message,
            "original_snapshot": original["current-snapshot-id"],
            "after_snapshot": observed["current-snapshot-id"],
        }),
    )?;
    assert_no_unsettled(context, conn)?;
    assert_content_refresh_explain(context, conn, "ContentLocate")?;
    refresh(context, conn, TARGET)?;
    assert_no_unsettled(context, conn)?;
    assert_independent_bags(context, conn)
}

fn lose_incarnation_registration(
    context: &mut ScenarioContext,
    conn: SqlSession,
    rest_uri: &str,
    control: &FixtureControl,
) -> Result<()> {
    let original = load_metadata(context, rest_uri, "ns", TARGET)?;
    let old_slot = read_management_slot(&original)?;
    let lost = control.fault_mv_commit(
        "ns",
        TARGET,
        RetainedMvCommit::ManagementSlot,
        PublicationFault::AfterCommitBeforeResponse,
    )?;
    drop(conn);
    restart_frontend(
        context,
        "lose the exact successor incarnation slot write response",
    )?;
    let mut conn = connect(context)?;
    select_catalog_and_database(context, &mut conn, CATALOG)?;
    require_nonaggregate_management_after_restart(context, &mut conn, CATALOG, TARGET)?;
    let lost = lost.finish()?;
    let registered = load_metadata(context, rest_uri, "ns", TARGET)?;
    let new_slot = read_management_slot(&registered)?;
    ensure!(
        old_slot["content"] != new_slot["content"]
            && new_slot["writer"].as_i64().is_some()
            && new_slot["writer"] == registered["current-snapshot-id"],
        "the observed successor slot is not the actual Current writer: {old_slot:?} -> {new_slot:?}"
    );
    record_lost_effect(
        context,
        lost,
        json!({"old_management_slot": old_slot, "new_management_slot": new_slot}),
    )?;
    assert_no_unsettled(context, &mut conn)?;
    assert_content_refresh_explain(context, &mut conn, "ContentLocate")?;
    refresh(context, &mut conn, TARGET)?;
    assert_no_unsettled(context, &mut conn)?;
    assert_independent_bags(context, &mut conn)
}

fn read_management_slot(metadata: &Value) -> Result<Value> {
    Ok(serde_json::from_str(
        metadata["properties"][MANAGEMENT_SLOT]
            .as_str()
            .context("managed target has no exact management slot")?,
    )?)
}

fn record_lost_effect(
    context: &ScenarioContext,
    evidence: FixtureFaultEvidence,
    mut facts: Value,
) -> Result<()> {
    let summary = evidence.summary();
    facts["lost_response"] = evidence.trace();
    fs::write(
        context.scenario_root().join("lost-mv-effect.json"),
        serde_json::to_vec_pretty(&facts)?,
    )?;
    ensure!(
        summary.matches("request-forwarded").count() == 1
            && summary.contains("downstream-success")
            && summary.contains("response-dropped-after-downstream-success"),
        "the original MV effect did not lose the response to one successful commit: {summary}"
    );
    Ok(())
}

fn assert_no_unsettled(context: &mut ScenarioContext, conn: &mut SqlSession) -> Result<()> {
    let observed = status(context, conn, CATALOG, TARGET)?;
    ensure!(
        property(&observed, "UnsettledEffects")? == "0",
        "the lost response installed an unsettled-effect barrier"
    );
    Ok(())
}

fn assert_independent_bags(context: &mut ScenarioContext, conn: &mut SqlSession) -> Result<()> {
    assert_rows(
        context,
        conn,
        "SELECT k1, v2 FROM orders_mv ORDER BY k1",
        &[(2, 20), (3, 30)],
        "read the MV after the next refresh",
    )?;
    execute(
        context,
        conn,
        "disable MV rewriting for the complete source result",
        "SET SESSION enable_materialized_view_rewrite = false",
    )?;
    assert_rows(
        context,
        conn,
        "SELECT k1, v2 FROM orders WHERE v2 > 0 ORDER BY k1",
        &[(2, 20), (3, 30)],
        "read the complete original source after the next refresh",
    )
}
