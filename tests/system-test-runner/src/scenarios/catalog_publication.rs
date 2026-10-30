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

//! Explicit publication properties exercised through native SQL and a real Java catalog.
//! The fixture observes exact service phases; the scenario owns every actor to actual exit.

use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::publication::PublicationFixture;
use novarocks_cluster_harness::publication_service::{CommitOutcome, Hold, HoldStage};
use novarocks_cluster_harness::{
    CrossProcessChildEnvironment, CrossProcessConfigOverlay, ServerHandle,
};
use serde_json::{Value, json};
use std::fs;
use std::path::Path;
use std::sync::Mutex;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

const CATALOG: &str = "publication_catalog";
const EVIDENCE_MS: u64 = 500;
const TABLE: &str = "publication_catalog.ns.orders";

#[derive(Clone, Copy)]
enum Property {
    LateCommit,
    StreamDeadline,
    ExpiryConcurrency,
    ManifestReprepare,
}

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    [
        Property::LateCommit,
        Property::StreamDeadline,
        Property::ExpiryConcurrency,
        Property::ManifestReprepare,
    ]
    .into_iter()
    .map(|property| {
        Box::new(PublicationScenario {
            property,
            fixture: Mutex::new(None),
        }) as Box<dyn Scenario>
    })
    .collect()
}
struct PublicationScenario {
    property: Property,
    fixture: Mutex<Option<PublicationFixture>>,
}
impl Scenario for PublicationScenario {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "catalog_publication",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        match self.property {
            Property::LateCommit => "catalog-publication/late-commit-observation",
            Property::StreamDeadline => "catalog-publication/stream-deadline",
            Property::ExpiryConcurrency => "catalog-publication/expiry-concurrency",
            Property::ManifestReprepare => "catalog-publication/manifest-reprepare",
        }
    }
    fn is_explicit_stage(&self) -> bool {
        true
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
        if matches!(self.property, Property::ExpiryConcurrency) {
            // Retain this FE's original file diagnostics with its scenario.
            let diagnostics = root.join("provider-diagnostics");
            fs::create_dir_all(&diagnostics)?;
            environment.fe.insert(
                "NOVAROCKS_LOG_DIR".into(),
                diagnostics.to_string_lossy().into_owned(),
            );
            environment.fe.insert(
                "NOVAROCKS_LOG_BASENAME".into(),
                "publication-maintenance".into(),
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
        // These races require two SQL actors to enter independently while one
        // synchronous publication is held. One fixed query worker serializes
        // them before either the catalog lock or the external CAS is tested.
        let frontend_overlay = if matches!(
            self.property,
            Property::ExpiryConcurrency | Property::ManifestReprepare
        ) {
            format!("{credential}\n[runtime]\nquery_blocking_worker_threads = 2\n")
        } else {
            credential.clone()
        };
        *slot = Some(fixture);
        Ok(ScenarioLaunchConfig {
            child_environment: environment,
            config_overlay: CrossProcessConfigOverlay {
                fe: Some(frontend_overlay),
                be: Some(credential.replace("object-store-metadata", "object-store-data")),
                ..Default::default()
            },
            ..Default::default()
        })
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "publication scenarios require native 1FE+3BE"
        );
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("publication fixture mutex poisoned"))?;
        let fixture = fixture
            .as_ref()
            .context("publication fixture not prepared")?;
        let budget = if matches!(
            self.property,
            Property::LateCommit | Property::StreamDeadline
        ) {
            EVIDENCE_MS
        } else {
            30_000
        };
        let connection = mysql_actor::connect_until(
            context.mysql_user(),
            context.mysql_port(),
            context.deadline(),
        )?;
        connection.run(|conn| {
            // Fixture preparation has a separate generous budget. Only the
            // selected publication consumes the deliberately short policy.
            conn.query_drop(catalog_sql(fixture, "publication_setup", 30_000))?;
            conn.query_drop("CREATE DATABASE publication_setup.ns")?;
            conn.query_drop("CREATE TABLE publication_setup.ns.orders (k INT) TBLPROPERTIES (\"format-version\"=\"2\", \"history.expire.min-snapshots-to-keep\"=\"1\")")?;
            conn.query_drop(catalog_sql(fixture, CATALOG, budget))?;
            match self.property {
                Property::LateCommit => late_commit(context, fixture),
                Property::StreamDeadline => stream_deadline(context, fixture),
                Property::ExpiryConcurrency | Property::ManifestReprepare => {
                    for value in 1..=4 { conn.query_drop(format!("INSERT INTO {TABLE} VALUES ({value})"))?; }
                    maintenance_race(context, fixture, self.property)?;
                    if matches!(self.property, Property::ExpiryConcurrency) {
                        expiry_properties(context, fixture)?;
                    }
                    Ok(())
                }
            }?;
            require_catalog_activity(context)
        })
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

fn require_catalog_activity(context: &ScenarioContext) -> Result<()> {
    let activity = reqwest::blocking::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()?
        .get(format!(
            "http://127.0.0.1:{}/v1/catalogs/activity",
            context.fe_http_port()
        ))
        .send()?
        .error_for_status()?
        .json::<Value>()?;
    save(context, "catalog-activity-active-source.json", &activity)?;
    ensure!(
        activity["schema_version"] == 1,
        "unexpected catalog activity schema"
    );
    let catalogs = activity["catalogs"]
        .as_array()
        .context("catalog activity has no source rows")?;
    ensure!(
        catalogs.iter().any(|row| {
            row["catalog"] == CATALOG
                && row["provider_id"] == "iceberg"
                && row["current"] == true
                && row["observation"].is_object()
        }),
        "active publication catalog has no provider observation: {activity}"
    );
    Ok(())
}

fn catalog_sql(fixture: &PublicationFixture, catalog: &str, budget: u64) -> String {
    let endpoints = fixture.rest().endpoints();
    format!(
        r#"CREATE EXTERNAL CATALOG {catalog} PROPERTIES(
        "type"="iceberg", "iceberg.catalog.type"="rest", "uri"="{}", "warehouse"="{}",
        "novarocks.publication.evidence-timeout-ms"="{budget}",
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
fn sql(context: &ScenarioContext, statement: &str) -> Result<()> {
    mysql_actor::connect_until(
        context.mysql_user(),
        context.mysql_port(),
        context.deadline(),
    )?
    .run(|conn| conn.query_drop(statement).map_err(Into::into))
}
fn metadata(fixture: &PublicationFixture) -> Result<Value> {
    table_metadata(fixture, "orders")
}
fn table_metadata(fixture: &PublicationFixture, table: &str) -> Result<Value> {
    Ok(reqwest::blocking::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()?
        .get(format!(
            "{}/v1/namespaces/ns/tables/{table}?snapshots=all",
            fixture.rest().endpoints().rest_uri
        ))
        .send()?
        .error_for_status()?
        .json::<Value>()?["metadata"]
        .clone())
}
fn save(context: &ScenarioContext, name: &str, value: &Value) -> Result<()> {
    fs::write(
        context.scenario_root().join(name),
        serde_json::to_vec_pretty(value)?,
    )?;
    Ok(())
}
fn require_unknown(outcome: &Result<()>) -> Result<()> {
    let error = outcome
        .as_ref()
        .err()
        .context("publication unexpectedly returned success")?;
    ensure!(
        format!("{error:#}")
            .to_ascii_lowercase()
            .contains("unknown"),
        "SQL did not return Unknown: {error:#}"
    );
    Ok(())
}
fn spawn_sql(context: &ScenarioContext, statement: String) -> JoinHandle<Result<()>> {
    let user = context.mysql_user().to_owned();
    let port = context.mysql_port();
    let deadline = context.deadline();
    std::thread::spawn(move || {
        mysql_actor::connect_until(&user, port, deadline)?
            .run(|conn| conn.query_drop(statement).map_err(Into::into))
    })
}
fn joined(query: JoinHandle<Result<()>>) -> Result<Result<()>> {
    query
        .join()
        .map_err(|_| anyhow::anyhow!("publication SQL actor panicked"))
}
fn aggregate(failures: &[(&str, Option<String>)]) -> Result<()> {
    let failures = failures
        .iter()
        .filter_map(|(stage, error)| error.as_ref().map(|error| format!("{stage}: {error}")))
        .collect::<Vec<_>>();
    ensure!(failures.is_empty(), "{}", failures.join("; "));
    Ok(())
}
fn failure<T>(result: &Result<T>) -> Option<String> {
    result.as_ref().err().map(|error| format!("{error:#}"))
}
/// Release, join and observe the exact Java wrapper independently, even when
/// the primary assertion or release fails. Persist all outcomes before return.
fn finish_service<T>(
    context: &ScenarioContext,
    name: &str,
    hold: &mut Hold,
    phase: Result<T>,
    query: JoinHandle<Result<()>>,
    join_while_held: bool,
    inspect_while_held: impl FnOnce(&Result<()>) -> Result<()>,
) -> Result<(T, Result<()>, CommitOutcome)> {
    let mut actor = Some(query);
    let mut outcome = None;
    if join_while_held && phase.is_ok() {
        outcome = Some(joined(actor.take().unwrap()));
    }
    let inspection = if let Some(Ok(result)) = &outcome {
        inspect_while_held(result)
    } else {
        Ok(())
    };
    let release = hold.release();
    let outcome = outcome.unwrap_or_else(|| joined(actor.take().unwrap()));
    let exited = hold.wait_for_original_exit(context.deadline());
    let trace = hold.trace().and_then(|trace| {
        fs::write(
            context.scenario_root().join(format!("{name}-trace.ndjson")),
            trace,
        )?;
        Ok(())
    });
    let receipt = save(
        context,
        &format!("{name}-actors.json"),
        &json!({
            "phase_error":failure(&phase),"held_inspection_error":failure(&inspection),"release_error":failure(&release),
            "join_error":failure(&outcome),"sql_actor_actual_exit":true,"sql_actor_join_succeeded":outcome.is_ok(),
            "sql_result":outcome.as_ref().ok().map(|result| format!("{result:?}")),
            "original_exit":format!("{exited:?}"),"trace_error":failure(&trace)
        }),
    );
    let sql_failure =
        if phase.is_err() || inspection.is_err() || release.is_err() || exited.is_err() {
            outcome.as_ref().ok().and_then(failure)
        } else {
            None
        };
    aggregate(&[
        ("phase", failure(&phase)),
        ("SQL outcome during failed cleanup", sql_failure),
        ("held inspection", failure(&inspection)),
        ("release", failure(&release)),
        ("join", failure(&outcome)),
        ("original actual exit", failure(&exited)),
        ("trace", failure(&trace)),
        ("actor receipt", failure(&receipt)),
    ])?;
    Ok((phase?, outcome?, exited?))
}
fn late_commit(context: &mut ScenarioContext, fixture: &PublicationFixture) -> Result<()> {
    let before = metadata(fixture)?;
    let control = fixture.proxy()?;
    let counts = control.mutation_counts("ns", "orders");
    let mut hold = fixture.service().arm("ns.orders")?;
    let user = context.mysql_user().to_owned();
    let port = context.mysql_port();
    let deadline = context.deadline();
    let started = Instant::now();
    let query = std::thread::spawn(move || {
        mysql_actor::connect_until(&user, port, deadline)?.run(|conn| {
            conn.query_drop(format!("ALTER TABLE {TABLE} ADD COLUMN late_value BIGINT"))
                .map_err(Into::into)
        })
    });
    let phase = (|| -> Result<()> {
        hold.wait_until_held(context.deadline())?;
        // The server has checked requirements but has not committed. The real
        // FE automatically reconciles against this still-old metadata.
        ensure!(
            metadata(fixture)? == before,
            "metadata advanced before held commit"
        );
        Ok(())
    })();
    let (_, _, exited) = finish_service(
        context,
        "service",
        &mut hold,
        phase,
        query,
        true,
        |query_result| {
            require_unknown(query_result)?;
            ensure!(
                started.elapsed() >= Duration::from_millis(EVIDENCE_MS * 4 / 5),
                "SQL completed before evidence budget"
            );
            ensure!(
                metadata(fixture)? == before,
                "automatic reconcile saw a newer metadata version"
            );
            ensure!(
                control
                    .mutation_counts("ns", "orders")
                    .table_commit_forwarded
                    == counts.table_commit_forwarded + 1,
                "Unknown dispatched another mutation"
            );
            ensure!(
                control
                    .mutation_counts("ns", "orders")
                    .table_load_while_commit_pending
                    > counts.table_load_while_commit_pending,
                "FE did not load old metadata while the exact original commit remained pending"
            );

            Ok(())
        },
    )?;
    ensure!(
        exited == CommitOutcome::Succeeded,
        "late original request did not commit"
    );
    let after = metadata(fixture)?;
    ensure!(
        after["current-schema-id"] != before["current-schema-id"],
        "late schema commit was not visible"
    );
    ensure!(
        control
            .mutation_counts("ns", "orders")
            .table_commit_forwarded
            == counts.table_commit_forwarded + 1,
        "late request was replayed"
    );
    save(
        context,
        "late-commit.json",
        &json!({"before":before,"after":after,"statement":"Unknown","original":"Succeeded","commit_method_actual_exit":true,"sql_actor_actual_exit":true,"evidence_budget_ms":EVIDENCE_MS}),
    )?;
    context.action("observed server requirements hold, SQL Unknown on old metadata, one dispatch, late success, and actual exit");
    sql(
        context,
        &catalog_sql(fixture, "publication_response", 30_000),
    )?;
    post_commit_reload(context, fixture)
}
fn post_commit_reload(context: &mut ScenarioContext, fixture: &PublicationFixture) -> Result<()> {
    let mut hold = fixture
        .service()
        .arm_at("ns.orders", HoldStage::AfterPersistentCommit)?;
    let user = context.mysql_user().to_owned();
    let port = context.mysql_port();
    let deadline = context.deadline();
    let query = std::thread::spawn(move || {
        mysql_actor::connect_until(&user, port, deadline)?.run(|conn| {
            conn.query_drop(
                "ALTER TABLE publication_response.ns.orders ADD COLUMN post_commit BIGINT",
            )
            .map_err(Into::into)
        })
    });
    let phase = (|| -> Result<Value> {
        hold.wait_until_held(context.deadline())?;
        let applied = metadata(fixture)?;
        reqwest::blocking::Client::builder().no_proxy().timeout(Duration::from_secs(5)).build()?
            .post(format!("{}/v1/namespaces/ns/tables/orders", fixture.rest().endpoints().rest_uri))
            .json(&json!({"requirements":[{"type":"assert-table-uuid","uuid":applied["table-uuid"]}],"updates":[{"action":"set-properties","updates":{"publication.competitor":"after-commit"}}]}))
            .send()?.error_for_status()?;
        Ok(applied)
    })();
    let (applied, outcome, exited) = finish_service(
        context,
        "post-commit",
        &mut hold,
        phase,
        query,
        false,
        |_| Ok(()),
    )?;
    outcome.context("post-commit response should preserve Applied evidence")?;
    ensure!(
        exited == CommitOutcome::Succeeded,
        "post-commit wrapper did not exit after success"
    );
    let after = metadata(fixture)?;
    ensure!(
        after["current-schema-id"] == applied["current-schema-id"],
        "handler reload lost the original schema"
    );
    ensure!(
        after["properties"]["publication.competitor"] == "after-commit",
        "handler reload did not observe the successor metadata"
    );
    save(
        context,
        "post-commit-reload.json",
        &json!({"applied":applied,"after":after,"original":"Succeeded","statement":"Applied","commit_method_actual_exit":true,"sql_actor_actual_exit":true}),
    )?;
    context.action("observed persisted commit before handler reload, successor publication, and original success exit");
    Ok(())
}

fn stream_deadline(context: &mut ScenarioContext, fixture: &PublicationFixture) -> Result<()> {
    let control = fixture.proxy()?;
    for (index, (action, statement, unknown)) in [
        (
            "table-commit",
            format!("ALTER TABLE {TABLE} ADD COLUMN streamed BIGINT"),
            true,
        ),
        (
            "catalog-mutation",
            format!("CREATE TABLE {CATALOG}.ns.created (k INT)"),
            true,
        ),
        (
            "catalog-mutation",
            format!("CREATE DATABASE {CATALOG}.streamed_namespace"),
            true,
        ),
        (
            "catalog-mutation",
            format!("CREATE VIEW {CATALOG}.ns.streamed_view AS SELECT 1 AS k"),
            true,
        ),
        (
            "catalog-mutation",
            format!("DROP VIEW {CATALOG}.ns.streamed_view"),
            false,
        ),
        (
            "catalog-mutation",
            format!("DROP TABLE {CATALOG}.ns.created"),
            false,
        ),
        (
            "catalog-mutation",
            format!("DROP DATABASE {CATALOG}.streamed_namespace"),
            false,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let before = control.traffic_snapshot();
        let guard = control.arm_next(action, "stream-response")?;
        let started = Instant::now();
        let outcome = sql(context, &statement);
        let elapsed = started.elapsed();
        // Preserve a trace even if finishing the body owner fails.
        let early_trace = save(
            context,
            &format!("stream-{index}-trace.json"),
            &guard.trace(),
        );
        let evidence = guard.finish();
        let after = control.traffic_snapshot();
        let trace_saved = match &evidence {
            Ok(evidence) => save(
                context,
                &format!("stream-{index}-trace.json"),
                evidence.trace(),
            ),
            Err(_) => early_trace,
        };
        let receipt = save(
            context,
            &format!("stream-{index}-result.json"),
            &json!({
                "statement":statement,"sql_result":format!("{outcome:?}"),
                "elapsed_ms":elapsed.as_millis(),"before":before,"after":after,
                "body_owner_error":failure(&evidence),"sql_actor_actual_exit":true
            }),
        );
        aggregate(&[
            ("body actual exit", failure(&evidence)),
            ("trace", failure(&trace_saved)),
            ("receipt", failure(&receipt)),
        ])?;
        let evidence = evidence?;
        let mutations =
            |counters: &novarocks_cluster_harness::publication_catalog::TrafficCounters| {
                counters
                    .by_method
                    .iter()
                    .filter(|(method, _)| !matches!(method.as_str(), "GET" | "HEAD" | "OPTIONS"))
                    .map(|(_, count)| *count)
                    .sum::<u64>()
            };
        ensure!(
            mutations(&after) == mutations(&before) + 1,
            "stream mutation was replayed or not forwarded exactly once: {statement}"
        );
        let trace = evidence.summary();
        ensure!(
            trace.contains("response-stream-exited"),
            "body owner did not actually exit: {trace}"
        );
        if unknown {
            require_unknown(&outcome)?;
            ensure!(
                trace.contains("response-stream-chunk"),
                "slow response never streamed: {trace}"
            );
            ensure!(
                elapsed >= Duration::from_millis(EVIDENCE_MS * 4 / 5)
                    && elapsed < Duration::from_secs(15),
                "slow response escaped total budget: {elapsed:?}"
            );
        } else {
            outcome.context("bodyless successful mutation lost Applied evidence")?;
        }
        context.action(format!("stream mutation {index}: one forwarded request, bounded SQL outcome, raw trace and body actual exit"));
    }
    Ok(())
}
fn maintenance_race(
    context: &mut ScenarioContext,
    fixture: &PublicationFixture,
    property: Property,
) -> Result<()> {
    let mut before = metadata(fixture)?;
    let protected_before = before["current-snapshot-id"]
        .as_i64()
        .context("current snapshot missing before maintenance race")?;
    if matches!(property, Property::ExpiryConcurrency) {
        // Freeze a requirement for this existing ref before the Java hold.
        // Moving it makes a handler retry fail that same requirement, so an
        // HTTP conflict must reach the provider before it can reprepare.
        set_ref(
            fixture,
            "orders",
            &before,
            "protected",
            "tag",
            Value::Null,
            protected_before,
        )?;
        before = metadata(fixture)?;
        ensure!(
            before["refs"]["protected"]["snapshot-id"] == protected_before,
            "protected ref was not established before maintenance preparation"
        );
    }
    let candidate = oldest(&before)?;
    if matches!(property, Property::ExpiryConcurrency) {
        ensure!(
            candidate != protected_before,
            "ref movement must change the frozen target"
        );
    }
    let candidate_list = before["snapshots"]
        .as_array()
        .unwrap()
        .iter()
        .find(|snapshot| snapshot["snapshot-id"] == candidate)
        .unwrap()["manifest-list"]
        .as_str()
        .context("candidate manifest list missing")?
        .to_owned();
    ensure!(
        fixture.rest().object_exists(&candidate_list)?,
        "candidate manifest list absent before expiry"
    );
    let control = fixture.proxy()?;
    let forwarded_before = control
        .mutation_counts("ns", "orders")
        .table_commit_forwarded;
    let traffic_before = control.traffic_snapshot();
    let mut hold = fixture.service().arm("ns.orders")?;
    let user = context.mysql_user().to_owned();
    let port = context.mysql_port();
    let deadline = context.deadline();
    let statement = if matches!(property, Property::ExpiryConcurrency) {
        format!(
            "ALTER TABLE {TABLE} EXPIRE SNAPSHOTS OLDER THAN '2099-01-01 00:00:00' RETAIN LAST 1"
        )
    } else {
        format!("ALTER TABLE {TABLE} REWRITE MANIFESTS")
    };
    let query = std::thread::spawn(move || {
        mysql_actor::connect_until(&user, port, deadline)?
            .run(|conn| conn.query_drop(statement).map_err(Into::into))
    });
    let mut competitor_snapshot = Value::Null;
    let mut competitor_list = String::new();
    let phase = (|| -> Result<()> {
        hold.wait_until_held(context.deadline())?;
        if matches!(property, Property::ExpiryConcurrency) {
            set_ref(
                fixture,
                "orders",
                &before,
                "protected",
                "tag",
                json!(protected_before),
                candidate,
            )?;
            let competitor = metadata(fixture)?;
            ensure!(
                competitor["refs"]["protected"]["snapshot-id"] == candidate,
                "external ref move was not visible before releasing maintenance"
            );
            save(
                context,
                "maintenance-competitor.json",
                &json!({"before":before,"competitor":competitor,"previous_target":protected_before,"moved_target":candidate}),
            )?;
        } else {
            // A distinct logical catalog is deliberately an external writer relative
            // to this catalog's local expiry/ref coordination domain.
            sql(
                context,
                &catalog_sql(fixture, "publication_competitor", 30_000),
            )?;
            sql(
                context,
                "INSERT INTO publication_competitor.ns.orders VALUES (99)",
            )?;
            let appended = metadata(fixture)?;
            competitor_snapshot = appended["current-snapshot-id"].clone();
            competitor_list = appended["snapshots"]
                .as_array()
                .context("append snapshots missing")?
                .iter()
                .find(|snapshot| snapshot["snapshot-id"] == competitor_snapshot)
                .context("append snapshot missing")?["manifest-list"]
                .as_str()
                .context("append manifest list missing")?
                .to_owned();
        }
        Ok(())
    })();
    let (_, outcome, original) = finish_service(
        context,
        "maintenance",
        &mut hold,
        phase,
        query,
        false,
        |_| Ok(()),
    )?;
    let traffic_after = control.traffic_snapshot();
    let forwarded = control
        .mutation_counts("ns", "orders")
        .table_commit_forwarded
        - forwarded_before;
    save(
        context,
        "maintenance-requests.json",
        &json!({"before":traffic_before,"after":traffic_after,"forwarded":forwarded,"sql_result":format!("{outcome:?}"),"original":format!("{original:?}")}),
    )?;
    ensure!(
        original == CommitOutcome::Conflict,
        "original maintenance request did not hit actual JDBC conflict"
    );
    outcome.context("maintenance did not successfully reprepare after definite conflict")?;
    if matches!(property, Property::ExpiryConcurrency) {
        ensure!(
            traffic_after
                .by_status
                .get(&409)
                .copied()
                .unwrap_or_default()
                == traffic_before
                    .by_status
                    .get(&409)
                    .copied()
                    .unwrap_or_default()
                    + 1,
            "existing ref move did not reach the provider as an actual REST 409"
        );
        ensure!(
            forwarded == 2,
            "existing ref conflict did not produce exactly two provider attempts"
        );
    }
    let after = metadata(fixture)?;
    let snapshots = after["snapshots"]
        .as_array()
        .context("result snapshots missing")?;
    if matches!(property, Property::ExpiryConcurrency) {
        ensure!(
            fixture.rest().object_exists(&candidate_list)?,
            "expiry deleted protected manifest list"
        );
        ensure!(
            after["refs"]["protected"]["snapshot-id"] == candidate,
            "expiry deleted the concurrently moved ref"
        );
        ensure!(
            snapshots
                .iter()
                .any(|snapshot| snapshot["snapshot-id"] == candidate),
            "expiry deleted protected snapshot"
        );
    } else {
        ensure!(
            fixture.rest().object_exists(&competitor_list)?,
            "rewrite deleted concurrent append manifest list"
        );
        ensure!(
            snapshots
                .iter()
                .any(|snapshot| snapshot["snapshot-id"] == competitor_snapshot),
            "rewrite lost concurrent append snapshot"
        );
    }
    let expected: Vec<i32> = if matches!(property, Property::ExpiryConcurrency) {
        vec![1, 2, 3, 4]
    } else {
        vec![1, 2, 3, 4, 99]
    };
    let actual: Vec<i32> = mysql_actor::connect_until(
        context.mysql_user(),
        context.mysql_port(),
        context.deadline(),
    )?
    .run(|conn| Ok(conn.query(format!("SELECT k FROM {TABLE} ORDER BY k"))?))?;
    ensure!(
        actual == expected,
        "maintenance race lost source rows: {actual:?}"
    );
    save(
        context,
        "maintenance-race.json",
        &json!({"before":before,"after":after,"protected_snapshot":candidate,"append_snapshot":competitor_snapshot,"rows":actual,"original":"Conflict","commit_method_actual_exit":true,"sql_actor_actual_exit":true}),
    )?;
    context.action("observed real server CAS conflict, successful reprepare, retained exact snapshot/ref IDs, and source readability");
    Ok(())
}

fn table_url(fixture: &PublicationFixture, table: &str) -> String {
    format!(
        "{}/v1/namespaces/ns/tables/{table}",
        fixture.rest().endpoints().rest_uri
    )
}
fn rest_client() -> Result<reqwest::blocking::Client> {
    Ok(reqwest::blocking::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()?)
}
fn seed(
    context: &ScenarioContext,
    fixture: &PublicationFixture,
    table: &str,
    format_version: u8,
) -> Result<Value> {
    sql(
        context,
        &format!(
            "CREATE TABLE publication_setup.ns.{table} (k INT) TBLPROPERTIES (\"format-version\"=\"{format_version}\", \"history.expire.min-snapshots-to-keep\"=\"1\")"
        ),
    )?;
    for value in 1..=3 {
        sql(
            context,
            &format!("INSERT INTO publication_setup.ns.{table} VALUES ({value})"),
        )?;
    }
    // The final overwrite makes the first snapshot's data unreachable from
    // main. Its later source read cannot pass merely through main protection.
    sql(
        context,
        &format!("INSERT OVERWRITE publication_setup.ns.{table} VALUES (4)"),
    )?;
    ensure!(
        rows(context, table, None)? == vec![4],
        "seed overwrite did not replace main data"
    );
    table_metadata(fixture, table)
}
fn snapshot_list(metadata: &Value, snapshot: i64) -> Result<String> {
    Ok(metadata["snapshots"]
        .as_array()
        .context("complete snapshots missing")?
        .iter()
        .find(|entry| entry["snapshot-id"] == snapshot)
        .context("exact snapshot missing")?["manifest-list"]
        .as_str()
        .context("manifest list missing")?
        .to_owned())
}
fn oldest(metadata: &Value) -> Result<i64> {
    metadata["snapshots"]
        .as_array()
        .context("snapshots missing")?
        .iter()
        .min_by_key(|snapshot| snapshot["sequence-number"].as_i64())
        .context("snapshots empty")?["snapshot-id"]
        .as_i64()
        .context("snapshot ID missing")
}
fn set_ref(
    fixture: &PublicationFixture,
    table: &str,
    before: &Value,
    name: &str,
    kind: &str,
    old: Value,
    target: i64,
) -> Result<()> {
    rest_client()?.post(table_url(fixture, table)).json(&json!({
        "requirements":[{"type":"assert-table-uuid","uuid":before["table-uuid"]},
            {"type":"assert-ref-snapshot-id","ref":name,"snapshot-id":old}],
        "updates":[{"action":"set-snapshot-ref","ref-name":name,"type":kind,"snapshot-id":target}]
    })).send()?.error_for_status()?;
    Ok(())
}
/// An independently registered, read-only source retains the pre-expiry
/// metadata pointer. GC cannot discover this other catalog entry; reading it
/// after expiry proves the actual old data files remain readable.
fn register_source(fixture: &PublicationFixture, table: &str) -> Result<String> {
    let response: Value = rest_client()?
        .get(table_url(fixture, table))
        .query(&[("snapshots", "all")])
        .send()?
        .error_for_status()?
        .json()?;
    let name = format!("source_{table}");
    rest_client()?
        .post(format!(
            "{}/v1/namespaces/ns/register",
            fixture.rest().endpoints().rest_uri
        ))
        .json(&json!({"name":name,"metadata-location":response["metadata-location"]}))
        .send()?
        .error_for_status()?;
    Ok(name)
}
fn rows(context: &ScenarioContext, table: &str, version: Option<&str>) -> Result<Vec<i32>> {
    let version = version
        .map(|version| format!(" FOR VERSION AS OF {version}"))
        .unwrap_or_default();
    mysql_actor::connect_until(
        context.mysql_user(),
        context.mysql_port(),
        context.deadline(),
    )?
    .run(|conn| {
        Ok(conn.query(format!(
            "SELECT k FROM {CATALOG}.ns.{table}{version} ORDER BY k"
        ))?)
    })
}
fn expire(catalog: &str, table: &str) -> String {
    format!(
        "ALTER TABLE {catalog}.ns.{table} EXPIRE SNAPSHOTS OLDER THAN '2099-01-01 00:00:00' RETAIN LAST 1"
    )
}
fn finish_proxy<T>(
    context: &ScenarioContext,
    name: &str,
    mut guard: novarocks_cluster_harness::publication_catalog::FixtureFaultGuard,
    phase: Result<T>,
    query: JoinHandle<Result<()>>,
) -> Result<(T, Result<()>)> {
    let release = guard.release();
    let outcome = joined(query);
    let early_trace = save(context, &format!("{name}-trace.json"), &guard.trace());
    let exit = guard.finish();
    let trace = match &exit {
        Ok(evidence) => save(context, &format!("{name}-trace.json"), evidence.trace()),
        Err(_) => early_trace,
    };
    let receipt = save(
        context,
        &format!("{name}-actors.json"),
        &json!({
            "phase_error":failure(&phase),"release_error":failure(&release),
            "join_error":failure(&outcome),"sql_actor_actual_exit":true,"sql_actor_join_succeeded":outcome.is_ok(),
            "sql_result":outcome.as_ref().ok().map(|result| format!("{result:?}")),
            "proxy_actual_exit_error":failure(&exit),"trace_error":failure(&trace)
        }),
    );
    let sql_failure = if phase.is_err() || release.is_err() || exit.is_err() {
        outcome.as_ref().ok().and_then(failure)
    } else {
        None
    };
    aggregate(&[
        ("phase", failure(&phase)),
        ("SQL outcome during failed cleanup", sql_failure),
        ("release", failure(&release)),
        ("join", failure(&outcome)),
        ("proxy actual exit", failure(&exit)),
        ("trace", failure(&trace)),
        ("actor receipt", failure(&receipt)),
    ])?;
    Ok((phase?, outcome?))
}
fn expiry_properties(context: &mut ScenarioContext, fixture: &PublicationFixture) -> Result<()> {
    for case in ["ref_move", "new_branch", "lost_tag"] {
        let table = format!("expiry_{case}");
        // Branch writes are admitted only for format v3; the other races
        // deliberately retain their existing format-v2 fixture.
        let format_version = if case == "new_branch" { 3 } else { 2 };
        let mut before = seed(context, fixture, &table, format_version)?;
        let candidate = oldest(&before)?;
        let candidate_list = snapshot_list(&before, candidate)?;
        let source = register_source(fixture, &table)?;
        ensure!(
            rows(context, &source, Some(&candidate.to_string()))? == vec![1],
            "source snapshot does not match the independently seeded first append"
        );
        let current = before["current-snapshot-id"]
            .as_i64()
            .context("current snapshot missing")?;
        if case == "ref_move" {
            set_ref(
                fixture,
                &table,
                &before,
                "moving",
                "tag",
                Value::Null,
                current,
            )?;
            before = table_metadata(fixture, &table)?;
        }
        let control = fixture.proxy()?;
        let count = control.mutation_counts("ns", &table).table_commit_forwarded;
        let traffic_before = control.traffic_snapshot();
        let guard = control.arm_next(
            "table-commit",
            "before-requirement-check-hold-for-concurrent-shell",
        )?;
        let query = spawn_sql(context, expire(CATALOG, &table));
        let mut branch_append_forwarded = None;
        let phase = (|| -> Result<(Value, Option<i64>)> {
            guard.wait_until_entered(context.deadline())?;
            match case {
                "ref_move" => set_ref(
                    fixture,
                    &table,
                    &before,
                    "moving",
                    "tag",
                    json!(current),
                    candidate,
                )?,
                "new_branch" => {
                    set_ref(
                        fixture,
                        &table,
                        &before,
                        "dev2",
                        "branch",
                        Value::Null,
                        candidate,
                    )?;
                    // This alias is a distinct logical catalog: an external writer
                    // relative to the expiry catalog's local coordination domain.
                    sql(
                        context,
                        &catalog_sql(fixture, "publication_branch_writer", 30_000),
                    )?;
                    let append_before =
                        control.mutation_counts("ns", &table).table_commit_forwarded;
                    sql(
                        context,
                        &format!(
                            "INSERT INTO publication_branch_writer.ns.{table}.branch_dev2 VALUES (99)"
                        ),
                    )?;
                    let append_forwarded =
                        control.mutation_counts("ns", &table).table_commit_forwarded
                            - append_before;
                    ensure!(
                        append_forwarded == 1,
                        "external branch append was not forwarded once"
                    );
                    branch_append_forwarded = Some(append_forwarded);
                }
                "lost_tag" => set_ref(
                    fixture,
                    &table,
                    &before,
                    "unexpected_tag",
                    "tag",
                    Value::Null,
                    candidate,
                )?,
                _ => unreachable!(),
            }
            let competitor = table_metadata(fixture, &table)?;
            let branch = competitor["refs"]["dev2"]["snapshot-id"].as_i64();
            save(
                context,
                &format!("{case}-before.json"),
                &json!({"before":before,"competitor":competitor,
                "candidate":candidate,"candidate_manifest_list":candidate_list,"independent_source":source}),
            )?;
            Ok((competitor, branch))
        })();
        let ((competitor, branch), outcome) = finish_proxy(context, case, guard, phase, query)?;
        let after = table_metadata(fixture, &table)?;
        let forwarded = control.mutation_counts("ns", &table).table_commit_forwarded - count;
        save(
            context,
            &format!("{case}-result.json"),
            &json!({"after":after,"sql_result":format!("{outcome:?}"),
            "forwarded":forwarded,"branch_append_forwarded":branch_append_forwarded,"candidate":candidate,"source":source}),
        )?;
        match case {
            "ref_move" => {
                outcome.context("expiry did not reprepare after moving an existing ref")?;
                let traffic_after = control.traffic_snapshot();
                ensure!(
                    traffic_after
                        .by_status
                        .get(&409)
                        .copied()
                        .unwrap_or_default()
                        == traffic_before
                            .by_status
                            .get(&409)
                            .copied()
                            .unwrap_or_default()
                            + 1,
                    "existing ref move did not cause an actual REST 409"
                );
                ensure!(
                    forwarded == 2,
                    "existing-ref conflict did not produce exactly two attempts"
                );
                ensure!(
                    after["refs"]["moving"]["snapshot-id"] == candidate,
                    "moved ref target lost"
                );
                ensure!(
                    snapshot_list(&after, candidate)? == candidate_list,
                    "moved ref snapshot lost"
                );
                ensure!(
                    fixture.rest().object_exists(&candidate_list)?,
                    "moved ref manifest list deleted"
                );
                ensure!(
                    rows(context, &table, Some("'moving'"))? == vec![1],
                    "moved ref source unreadable"
                );
            }
            "new_branch" => {
                outcome.context("expiry failed with a new external branch")?;
                let append_forwarded = branch_append_forwarded
                    .context("external branch append forwarding was not observed")?;
                ensure!(
                    forwarded == append_forwarded + 1,
                    "new branch race did not forward expiry exactly once in addition to its external append"
                );
                let branch = branch.context("external branch snapshot absent")?;
                ensure!(
                    after["refs"]["dev2"]["snapshot-id"] == branch,
                    "external branch target lost"
                );
                let list = snapshot_list(&competitor, branch)?;
                ensure!(
                    snapshot_list(&after, branch)? == list
                        && fixture.rest().object_exists(&list)?,
                    "external branch manifest list lost"
                );
                ensure!(
                    rows(context, &table, Some("'dev2'"))? == vec![1, 99],
                    "external branch shared source files lost"
                );
            }
            "lost_tag" => {
                // Expiration committed and necessary finalization completed.
                // Retained GC failure is independent of the successful SQL result.
                outcome.context("Applied expiry was changed into failure by retained GC")?;
                let committed_failure =
                    "Expiration Applied with unexpectedly lost refs (files protected): ";
                let lost_ref =
                    format!("unexpected_tag: snapshot={candidate}, manifest-list={candidate_list}");
                let diagnostic = loop {
                    let diagnostic_path = context
                        .scenario_root()
                        .join("provider-diagnostics/publication-maintenance.WARNING");
                    let log = fs::read_to_string(&diagnostic_path).with_context(|| {
                        format!("read original FE diagnostic {}", diagnostic_path.display())
                    })?;
                    if let Some(line) = log.lines().find(|line| {
                        line.contains("committed metadata maintenance retains incomplete cleanup")
                            && line.contains(committed_failure)
                            && line.contains(&lost_ref)
                    }) {
                        break line.to_owned();
                    }
                    context.remaining("observe original lost-ref cleanup diagnostic")?;
                    std::thread::sleep(Duration::from_millis(20));
                };
                save(
                    context,
                    "lost_tag-cleanup-diagnostic.json",
                    &json!({"diagnostic":diagnostic,"candidate":candidate,
                        "candidate_manifest_list":candidate_list}),
                )?;
                ensure!(forwarded == 1, "Applied lost-tag mutation was replayed");
                ensure!(
                    after["refs"].get("unexpected_tag").is_none(),
                    "fixture did not produce a lost tag"
                );
                ensure!(
                    after["snapshots"]
                        .as_array()
                        .context("snapshots missing")?
                        .iter()
                        .all(|snapshot| snapshot["snapshot-id"] != candidate),
                    "candidate was not actually expired"
                );
                ensure!(
                    fixture.rest().object_exists(&candidate_list)?,
                    "lost tag manifest list deleted"
                );
                ensure!(
                    rows(context, &source, Some(&candidate.to_string()))? == vec![1],
                    "lost tag source files unreadable"
                );
            }
            _ => unreachable!(),
        }
        ensure!(
            rows(context, &table, None)? == vec![4],
            "expiry changed main source rows"
        );
        context.action(format!("expiry {case}: exact snapshot/ref IDs, reachable manifest list and independent source read verified"));
    }
    local_expiry_serialization(context, fixture)
}
fn wait_for_statement(context: &ScenarioContext, statement: &str) -> Result<()> {
    loop {
        let present = mysql_actor::connect_until(
            context.mysql_user(),
            context.mysql_port(),
            context.deadline(),
        )?
        .run(|conn| {
            let rows: Vec<mysql::Row> = conn.query("SHOW FULL PROCESSLIST")?;
            Ok(rows.iter().any(|row| {
                row.get::<Option<String>, _>("Info").flatten().as_deref() == Some(statement)
            }))
        })?;
        if present {
            return Ok(());
        }
        ensure!(
            Instant::now() < context.deadline(),
            "local ref never reached FE statement ownership"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}
fn local_expiry_serialization(
    context: &mut ScenarioContext,
    fixture: &PublicationFixture,
) -> Result<()> {
    let table = "expiry_local_serial";
    let before = seed(context, fixture, table, 2)?;
    let candidate = oldest(&before)?;
    let control = fixture.proxy()?;
    let initial_count = control.mutation_counts("ns", table).table_commit_forwarded;
    let mut hold = fixture.service().arm(&format!("ns.{table}"))?;
    let query = spawn_sql(context, expire(CATALOG, table));
    let mut reference = None;
    let phase = (|| -> Result<()> {
        hold.wait_until_held(context.deadline())?;
        let count = control.mutation_counts("ns", table).table_commit_forwarded;
        let statement = format!(
            "ALTER TABLE {CATALOG}.ns.{table} CREATE TAG local_waiter AS OF VERSION {candidate}"
        );
        reference = Some(spawn_sql(context, statement.clone()));
        wait_for_statement(context, &statement)?;
        std::thread::sleep(Duration::from_millis(250));
        ensure!(
            !reference.as_ref().unwrap().is_finished(),
            "local ref did not wait for expiry conclusion"
        );
        ensure!(
            control.mutation_counts("ns", table).table_commit_forwarded == count,
            "same-catalog ref dispatched while definite expiry was held"
        );
        ensure!(
            table_metadata(fixture, table)?["refs"]
                .get("local_waiter")
                .is_none(),
            "local ref became visible before expiry conclusion"
        );
        Ok(())
    })();
    let expired = finish_service(
        context,
        "local-serialization",
        &mut hold,
        phase,
        query,
        false,
        |_| Ok(()),
    );
    // Join the waiting ref even if release, the expiry actor or wrapper failed.
    let reference = reference.map(joined).transpose();
    let receipt = save(
        context,
        "local-serialization-ref.json",
        &json!({
            "expiry":format!("{expired:?}"),"ref_actor":format!("{reference:?}"),
            "ref_actor_actual_exit":!matches!(&reference,Ok(None))
        }),
    );
    aggregate(&[
        ("expiry", failure(&expired)),
        ("ref join", failure(&reference)),
        ("receipt", failure(&receipt)),
    ])?;
    let (_, expired, original) = expired?;
    expired.context("local serialized expiry did not apply")?;
    ensure!(
        original == CommitOutcome::Succeeded,
        "local expiry did not actually commit"
    );
    let rejected = reference?
        .context("ref actor not started")?
        .err()
        .context("ref planned against an already expired snapshot after lock release")?;
    let diagnostic = format!("{rejected:#}");
    ensure!(
        diagnostic.to_ascii_lowercase().contains("snapshot")
            && diagnostic.contains(&candidate.to_string()),
        "local ref failed for a reason other than the removed snapshot: {diagnostic}"
    );
    ensure!(
        control.mutation_counts("ns", table).table_commit_forwarded == initial_count + 1,
        "local ref dispatched against the removed snapshot"
    );
    let after = table_metadata(fixture, table)?;
    ensure!(
        after["refs"].get("local_waiter").is_none() && snapshot_list(&after, candidate).is_err(),
        "serialized ref restored an expired snapshot"
    );
    ensure!(
        rows(context, table, None)? == vec![4],
        "serialized expiry changed source rows"
    );
    context.action("same-catalog ref was FE-owned, did not dispatch while expiry was held, then rejected the removed snapshot");

    // Unknown releases the same logical catalog lock. A new local ref can
    // publish before the original, already dispatched request takes effect.
    let table = "expiry_unknown_window";
    let before = seed(context, fixture, table, 2)?;
    let candidate = oldest(&before)?;
    let list = snapshot_list(&before, candidate)?;
    let source = register_source(fixture, table)?;
    sql(
        context,
        &catalog_sql(fixture, "publication_unknown", EVIDENCE_MS),
    )?;
    let unknown_count = control.mutation_counts("ns", table).table_commit_forwarded;
    let service_table = format!("ns.{table}");
    let mut hold = fixture.service().arm(&service_table)?;
    let query = spawn_sql(context, expire("publication_unknown", table));
    let phase = (|| -> Result<Value> {
        hold.wait_until_held(context.deadline())?;
        let events = hold
            .trace()?
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let held = events
            .iter()
            .filter(|event| event["table"] == service_table && event["event"] == "hold-reached")
            .collect::<Vec<_>>();
        ensure!(
            held.len() == 1,
            "Unknown window has no unique original Java hold"
        );
        Ok((*held[0]).clone())
    })();
    let original_identity = phase.as_ref().ok().cloned();
    let finished = finish_service(
        context,
        "unknown-window-service",
        &mut hold,
        phase,
        query,
        true,
        |outcome| {
            require_unknown(outcome)?;
            ensure!(
                format!("{:#}", outcome.as_ref().unwrap_err()).contains("[CommitUnknown]"),
                "Unknown window SQL lost the canonical CommitUnknown code"
            );
            sql(
                context,
                &format!(
                    "ALTER TABLE publication_unknown.ns.{table} CREATE TAG after_unknown AS OF VERSION {candidate}"
                ),
            )?;
            let tagged = table_metadata(fixture, table)?;
            ensure!(
                tagged["refs"]["after_unknown"]["snapshot-id"] == candidate,
                "same-catalog ref did not publish after Unknown released the lock"
            );
            save(context, "unknown-window-before-release.json", &tagged)
        },
    );
    // The original wrapper's exit is only the first Java CAS attempt. The
    // same HTTP handler may refresh and retry the unchanged updates after it.
    // Bind that successor to the held table/thread and its own commit ID;
    // metadata loss alone cannot prove its actual exit.
    let retry = (|| -> Result<Value> {
        let original = original_identity
            .as_ref()
            .context("Unknown window never acquired an original Java identity")?;
        let thread = original["thread"].as_str().context("held thread missing")?;
        let commit = original["commit_id"]
            .as_str()
            .context("held commit ID missing")?;
        let arm = original["arm_id"].as_str().context("held arm ID missing")?;
        ensure!(
            !thread.is_empty() && !commit.is_empty() && !arm.is_empty(),
            "original Java hold identity is empty"
        );
        loop {
            let events = hold
                .trace()?
                .lines()
                .map(serde_json::from_str::<Value>)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let own = |event: &&Value| {
                event["table"] == service_table
                    && event["thread"] == thread
                    && event["commit_id"] == commit
                    && event["arm_id"] == arm
            };
            let conflict = events
                .iter()
                .filter(|event| own(event))
                .find(|event| event["event"] == "delegate-commit-conflict");
            let exited = events
                .iter()
                .filter(|event| own(event))
                .find(|event| event["event"] == "commit-method-exited");
            if let Some(exited) = exited {
                let conflict = conflict.context(
                    "original Java method exited without the expected tag-induced conflict",
                )?;
                let conflict_sequence = conflict["sequence"]
                    .as_u64()
                    .context("conflict sequence missing")?;
                let exit_sequence = exited["sequence"]
                    .as_u64()
                    .context("original exit sequence missing")?;
                ensure!(
                    conflict_sequence < exit_sequence,
                    "original Java exit preceded its conflict"
                );
                let successors = events
                    .iter()
                    .filter(|event| {
                        event["table"] == service_table
                            && event["thread"] == thread
                            && event["arm_id"] == ""
                            && event["commit_id"] != commit
                            && event["event"] == "requirements-passed-before-persistent-commit"
                            && event["sequence"]
                                .as_u64()
                                .is_some_and(|sequence| sequence > exit_sequence)
                    })
                    .collect::<Vec<_>>();
                ensure!(
                    successors.len() <= 1,
                    "Unknown window has multiple Java retry identities"
                );
                if let Some(successor) = successors.first() {
                    let retry_commit = successor["commit_id"]
                        .as_str()
                        .context("retry commit ID missing")?;
                    ensure!(!retry_commit.is_empty(), "retry commit ID is empty");
                    let entered = successor["sequence"]
                        .as_u64()
                        .context("retry sequence missing")?;
                    let refresh = events.iter().find(|event| {
                        event["table"] == service_table
                            && event["thread"] == thread
                            && event["event"] == "refresh"
                            && event["sequence"].as_u64().is_some_and(|sequence| {
                                sequence > exit_sequence && sequence < entered
                            })
                    });
                    ensure!(
                        refresh.is_some(),
                        "Java retry did not refresh after original actual exit"
                    );
                    let successor_events = events
                        .iter()
                        .filter(|event| {
                            event["table"] == service_table
                                && event["thread"] == thread
                                && event["arm_id"] == ""
                                && event["commit_id"] == retry_commit
                        })
                        .collect::<Vec<_>>();
                    if let Some(retry_exit) = successor_events
                        .iter()
                        .find(|event| event["event"] == "commit-method-exited")
                    {
                        let started = successor_events
                            .iter()
                            .find(|event| event["event"] == "delegate-commit-start")
                            .context("Java retry actually exited without delegate start")?;
                        let success = successor_events
                            .iter()
                            .find(|event| event["event"] == "delegate-commit-success")
                            .context("Java retry actually exited without success")?;
                        let start_sequence = started["sequence"]
                            .as_u64()
                            .context("retry start sequence missing")?;
                        let success_sequence = success["sequence"]
                            .as_u64()
                            .context("retry success sequence missing")?;
                        let retry_exit_sequence = retry_exit["sequence"]
                            .as_u64()
                            .context("retry exit sequence missing")?;
                        ensure!(
                            entered < start_sequence
                                && start_sequence < success_sequence
                                && success_sequence < retry_exit_sequence,
                            "Java retry actual exit did not follow its successful delegate"
                        );
                        return Ok(
                            json!({"original":original,"original_conflict":conflict,"original_exit":exited,
                            "refresh":refresh,"retry_entered":successor,"retry_start":started,"retry_success":success,"retry_exit":retry_exit}),
                        );
                    }
                }
            }
            ensure!(
                Instant::now() < context.deadline(),
                "Unknown window Java retry actual exit was not observed"
            );
            std::thread::sleep(Duration::from_millis(25));
        }
    })();
    // Capture the final trace even when the lifecycle proof or SQL inspection
    // failed; finish_service already attempted release/join/original exit.
    let trace = hold.trace().and_then(|trace| {
        fs::write(
            context.scenario_root().join("unknown-window-trace.ndjson"),
            trace,
        )?;
        Ok(())
    });
    let after = table_metadata(fixture, table);
    let forwarded = control.mutation_counts("ns", table).table_commit_forwarded - unknown_count;
    let receipt = save(
        context,
        "unknown-window-result.json",
        &json!({
            "service_actors":format!("{finished:?}"),"service_error":failure(&finished),
            "retry":retry.as_ref().ok(),"retry_error":failure(&retry),
            "after":after.as_ref().ok(),"candidate":candidate,"manifest_list":list,
            "forwarded":forwarded,
            "sql_actor_actual_exit":true,"original_and_retry_actual_exit":retry.is_ok(),
            "guarantee":"local serialization ends at Unknown; late application remains possible"
        }),
    );
    aggregate(&[
        ("service release/join/original exit", failure(&finished)),
        ("retry actual exit", failure(&retry)),
        ("trace", failure(&trace)),
        ("metadata", failure(&after)),
        ("receipt", failure(&receipt)),
    ])?;
    let after = after?;
    ensure!(
        forwarded == 2,
        "Unknown expiry or the following local tag was replayed"
    );
    ensure!(
        after["refs"].get("after_unknown").is_none() && snapshot_list(&after, candidate).is_err(),
        "fixture did not demonstrate the late expiry loss window after Unknown"
    );
    ensure!(
        fixture.rest().object_exists(&list)?,
        "Unknown expiry deleted a candidate manifest list"
    );
    ensure!(
        rows(context, &source, Some(&candidate.to_string()))? == vec![1],
        "Unknown expiry deleted source data files"
    );
    ensure!(
        rows(context, table, None)? == vec![4],
        "late expiry changed main source rows"
    );
    context.action("Unknown unlocked the same catalog; local tag applied before original late expiry removed it, while physical files remained readable");
    Ok(())
}
