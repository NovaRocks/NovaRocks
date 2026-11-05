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

use crate::actors::sql::{SqlSession, StatementFailure, StatementResult, TrinoValue};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::publication::PublicationFixture;
use novarocks_cluster_harness::publication_service::{CommitOutcome, Hold, HoldStage};
use novarocks_cluster_harness::{
    CrossProcessChildEnvironment, CrossProcessConfigOverlay, QueryLifecycleStatementObservation,
    StatementObservationOnly, TestClusterOwner,
    test_sessions::{ClusterTestSessionPlan, RoleSlot},
};
use novarocks_test_trino_client::{CancelHandle, PendingStatement, TrinoError};
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
            statement_observer: Mutex::new(None),
        }) as Box<dyn Scenario>
    })
    .collect()
}
struct PublicationScenario {
    property: Property,
    fixture: Mutex<Option<PublicationFixture>>,
    statement_observer: Mutex<Option<StatementObservationOnly>>,
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
    fn test_session_plan(
        &self,
        _: &ScenarioLaunchConfig,
        _: &Path,
        size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<ClusterTestSessionPlan>> {
        if !matches!(self.property, Property::ExpiryConcurrency) {
            return Ok(None);
        }
        ensure!(size == 3, "publication observation requires native 1FE+3BE");
        let mut observer = self
            .statement_observer
            .lock()
            .map_err(|_| anyhow::anyhow!("publication statement observer poisoned"))?;
        ensure!(observer.is_none(), "publication observer already reserved");
        let mut plan = ClusterTestSessionPlan::new(deadline)?;
        let (owner, grant) = StatementObservationOnly::reserve(&plan)?;
        let frontend = plan.reserve_launch(RoleSlot::Frontend)?;
        plan.authorize_launch(
            &frontend,
            vec![grant],
            vec![
                ("novarocks-server".into(), "test-controls".into()),
                (
                    "novarocks-frontend-application".into(),
                    "test-controls".into(),
                ),
            ],
        )?;
        plan.set_launch_binary(&frontend, binary.to_path_buf())?;
        for index in 0..size {
            let backend = plan.reserve_launch(RoleSlot::Backend(index))?;
            plan.authorize_ordinary_launch(&backend, binary.to_path_buf())?;
        }
        *observer = Some(owner);
        Ok(Some(plan))
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
        let deadline = context.deadline();
        let mut conn = new_session(context)?;
        // Fixture preparation has a separate generous budget. Only the
        // selected publication consumes the deliberately short policy.
        execute_sql(
            &mut conn,
            catalog_sql(fixture, "publication_setup", 30_000),
            deadline,
        )?;
        execute_sql(&mut conn, "CREATE DATABASE publication_setup.ns", deadline)?;
        execute_sql(
            &mut conn,
            "CREATE TABLE publication_setup.ns.orders (k INT) TBLPROPERTIES (\"format-version\"=\"2\", \"history.expire.min-snapshots-to-keep\"=\"1\")",
            deadline,
        )?;
        execute_sql(&mut conn, catalog_sql(fixture, CATALOG, budget), deadline)?;
        match self.property {
            Property::LateCommit => late_commit(context, fixture),
            Property::StreamDeadline => stream_deadline(context, fixture),
            Property::ExpiryConcurrency | Property::ManifestReprepare => {
                for value in 1..=4 {
                    execute_sql(
                        &mut conn,
                        format!("INSERT INTO {TABLE} VALUES ({value})"),
                        deadline,
                    )?;
                }
                maintenance_race(context, fixture, self.property)?;
                if matches!(self.property, Property::ExpiryConcurrency) {
                    let observer = self
                        .statement_observer
                        .lock()
                        .map_err(|_| anyhow::anyhow!("publication statement observer poisoned"))?
                        .take()
                        .context("publication statement observer not reserved")?;
                    expiry_properties(context, fixture, observer)?;
                }
                Ok(())
            }
        }?;
        require_catalog_activity(context)
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
fn new_session(context: &ScenarioContext) -> Result<SqlSession> {
    Ok(SqlSession::new(
        context.sql_user(),
        context.trino_port(),
        context.deadline(),
    )?)
}
fn execute_sql(
    session: &mut SqlSession,
    statement: impl AsRef<str>,
    deadline: Instant,
) -> Result<StatementResult> {
    let pending = session.submit(statement)?;
    let cancel = pending.cancel_handle();
    let outcome = session.run_pending(pending);
    let cleanup = if matches!(&outcome, Err(error) if !matches!(error, StatementFailure::Trino(_)))
    {
        cancel.delete(deadline).map_err(anyhow::Error::from)
    } else {
        Ok(())
    };
    both(outcome.map_err(anyhow::Error::from), cleanup).map(|(result, ())| result)
}
fn sql(context: &ScenarioContext, statement: &str) -> Result<()> {
    execute_sql(&mut new_session(context)?, statement, context.deadline()).map(|_| ())
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
fn public_sql_error(error: &anyhow::Error) -> Result<&TrinoError> {
    match error
        .chain()
        .find_map(|source| source.downcast_ref::<StatementFailure>())
    {
        Some(StatementFailure::Trino(error)) => Ok(error),
        Some(failure) => {
            Err(anyhow::Error::new(failure.clone()).context("expected original Trino SQL failure"))
        }
        None => anyhow::bail!("publication failure has no original Trino SQL source: {error:#}"),
    }
}
fn require_unknown(outcome: &Result<()>) -> Result<()> {
    let error = outcome
        .as_ref()
        .err()
        .context("publication unexpectedly returned success")?;
    let wire = public_sql_error(error)?;
    if !wire.message.to_ascii_lowercase().contains("unknown") {
        return Err(anyhow::Error::new(StatementFailure::Trino(wire.clone()))
            .context("SQL did not return Unknown"));
    }
    Ok(())
}

/// Owns the one actual POST, its same cancellation capability and real reader.
struct PublicationSqlActor {
    cancel: CancelHandle,
    deadline: Instant,
    join: Option<JoinHandle<std::result::Result<StatementResult, StatementFailure>>>,
    join_succeeded: Option<bool>,
    settled: Option<Result<Result<()>>>,
}
impl PublicationSqlActor {
    fn start(
        mut session: SqlSession,
        pending: PendingStatement,
        deadline: Instant,
    ) -> Result<Self> {
        let cancel = pending.cancel_handle();
        match std::thread::Builder::new()
            .name("publication-sql".into())
            .spawn(move || session.run_pending(pending))
        {
            Ok(join) => Ok(Self {
                cancel,
                deadline,
                join: Some(join),
                join_succeeded: None,
                settled: None,
            }),
            Err(error) => both(
                Err(anyhow::Error::from(error)),
                cancel.delete(deadline).map_err(Into::into),
            )
            .map(|(actor, ())| actor),
        }
    }
    fn is_finished(&self) -> bool {
        self.join.as_ref().is_none_or(JoinHandle::is_finished)
    }
    fn cancel_active(&self) -> Result<()> {
        if self.join.as_ref().is_some_and(|join| !join.is_finished()) {
            self.cancel.delete(self.deadline)?;
        }
        Ok(())
    }
    fn prejoin_finished(&mut self) {
        if self.settled.is_none() && self.join.as_ref().is_some_and(JoinHandle::is_finished) {
            // This consumes the actual reader once, including same-query DELETE
            // on non-Trino refusal. Keep its original outcome for the owner.
            self.settled = Some(self.join());
        }
    }
    fn join(&mut self) -> Result<Result<()>> {
        if let Some(settled) = self.settled.take() {
            return settled;
        }
        let joined = self
            .join
            .take()
            .context("publication SQL actor already joined")?
            .join();
        self.join_succeeded = Some(joined.is_ok());
        let terminal = matches!(&joined, Ok(Ok(_)) | Ok(Err(StatementFailure::Trino(_))));
        // A local/HTTP/transport failure or panic is not server terminal,
        // including after this original reader has actually returned.
        let cleanup = if terminal {
            Ok(())
        } else {
            self.cancel.delete(self.deadline).map_err(Into::into)
        };
        let response = joined
            .map_err(|payload| {
                anyhow::Error::new(PublicationSqlPanic {
                    _original_payload: Mutex::new(payload),
                })
            })
            .map(|response| response.map(|_| ()).map_err(anyhow::Error::from));
        match cleanup {
            Ok(()) => response,
            Err(cleanup) => match response {
                Ok(Err(primary)) | Err(primary) => Err(PublicationFailures {
                    primary,
                    secondary: cleanup,
                }
                .into()),
                Ok(Ok(())) => Err(cleanup),
            },
        }
    }
}
impl Drop for PublicationSqlActor {
    fn drop(&mut self) {
        if self.join.is_some() || self.settled.is_some() {
            let cancelled = self.cancel_active();
            let joined = self.join().and_then(|outcome| outcome);
            if let Err(error) = both(joined, cancelled) {
                eprintln!("publication SQL actor cleanup failed: {error:#}");
            }
        }
    }
}
struct PublicationSqlPanic {
    _original_payload: Mutex<Box<dyn std::any::Any + Send + 'static>>,
}
impl std::fmt::Debug for PublicationSqlPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PublicationSqlPanic(original payload retained)")
    }
}
impl std::fmt::Display for PublicationSqlPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("publication SQL actor panicked (original payload retained)")
    }
}
impl std::error::Error for PublicationSqlPanic {}
#[derive(Debug)]
struct PublicationFailures {
    primary: anyhow::Error,
    secondary: anyhow::Error,
}
impl std::fmt::Display for PublicationFailures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:#}; additional publication failure: {:#}",
            self.primary, self.secondary
        )
    }
}
impl std::error::Error for PublicationFailures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.primary.as_ref())
    }
}
fn both<T, U>(primary: Result<T>, secondary: Result<U>) -> Result<(T, U)> {
    match (primary, secondary) {
        (Ok(a), Ok(b)) => Ok((a, b)),
        (Err(error), Ok(_)) | (Ok(_), Err(error)) => Err(error),
        (Err(primary), Err(secondary)) => Err(PublicationFailures { primary, secondary }.into()),
    }
}
fn spawn_sql(context: &ScenarioContext, statement: String) -> Result<PublicationSqlActor> {
    let session = new_session(context)?;
    let pending = session.submit(statement)?;
    PublicationSqlActor::start(session, pending, context.deadline())
}
fn spawn_observed_sql(
    context: &mut ScenarioContext,
    statement: String,
    observer: StatementObservationOnly,
    observation_slot: &mut Option<QueryLifecycleStatementObservation>,
) -> Result<PublicationSqlActor> {
    ensure!(
        observation_slot.is_none(),
        "publication local reference was already nominated"
    );
    let deadline = context.deadline();
    let session = new_session(context)?;
    let pending = session.submit(&statement)?;
    let cancel = pending.cancel_handle();
    let observation = match observer.arm(context.handle(), &pending.query_id, &statement, deadline)
    {
        Ok(observation) => observation,
        Err(primary) => {
            return both(
                Err::<(), _>(primary),
                cancel.delete(deadline).map_err(Into::into),
            )
            .map(|_| unreachable!());
        }
    };
    // The successful nomination remains owned even if its original reader
    // cannot start. Cleanup must drain only this actual arm, after settlement.
    *observation_slot = Some(observation);
    PublicationSqlActor::start(session, pending, deadline)
}
fn service_sql(
    context: &ScenarioContext,
    name: &str,
    hold: &mut Hold,
    statement: String,
) -> Result<PublicationSqlActor> {
    match spawn_sql(context, statement) {
        Ok(actor) => Ok(actor),
        Err(primary) => {
            let release = hold.release();
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
                    "actor_start_error":format!("{primary:#}"), "release_error":failure(&release),
                    "trace_error":failure(&trace), "sql_actor_started":false,
                    "sql_actor_actual_exit":null, "original_exit":null
                }),
            );
            both(Err::<(), _>(primary), both(release, both(trace, receipt))).map(|_| unreachable!())
        }
    }
}
fn proxy_sql(
    context: &ScenarioContext,
    name: &str,
    mut guard: novarocks_cluster_harness::publication_catalog::FixtureFaultGuard,
    statement: String,
) -> Result<(
    PublicationSqlActor,
    novarocks_cluster_harness::publication_catalog::FixtureFaultGuard,
)> {
    match spawn_sql(context, statement) {
        Ok(actor) => Ok((actor, guard)),
        Err(primary) => {
            let release = guard.release();
            let trace = save(context, &format!("{name}-trace.json"), &guard.trace());
            let exit = guard.finish();
            let receipt = save(
                context,
                &format!("{name}-actors.json"),
                &json!({
                    "actor_start_error":format!("{primary:#}"), "release_error":failure(&release),
                    "trace_error":failure(&trace), "proxy_actual_exit_error":failure(&exit),
                    "sql_actor_started":false, "sql_actor_actual_exit":null
                }),
            );
            both(
                Err::<(), _>(primary),
                both(release, both(trace, both(exit, receipt))),
            )
            .map(|_| unreachable!())
        }
    }
}
/// Preserve actual SQL as the primary source whenever another owned phase fails.
fn finish_query<T>(phase: Result<T>, outcome: Result<Result<()>>) -> Result<(T, Result<()>)> {
    match (phase, outcome) {
        (Ok(phase), Ok(outcome)) => Ok((phase, outcome)),
        (Err(error), Ok(Ok(()))) | (Ok(_), Err(error)) => Err(error),
        (Err(secondary), Ok(Err(primary)) | Err(primary)) => {
            Err(PublicationFailures { primary, secondary }.into())
        }
    }
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
    mut query: PublicationSqlActor,
    join_while_held: bool,
    peer: Option<&mut PublicationSqlActor>,
    inspect_while_held: impl FnOnce(&Result<()>) -> Result<()>,
) -> Result<(T, Result<()>, CommitOutcome)> {
    let mut outcome = None;
    if join_while_held && phase.is_ok() {
        outcome = Some(query.join());
    }
    let inspection = if let Some(Ok(result)) = &outcome {
        inspect_while_held(result)
    } else {
        Ok(())
    };
    let release = hold.release();
    let failed_before_join = phase.is_err() || inspection.is_err() || release.is_err();
    let cancellation = if failed_before_join {
        query.cancel_active()
    } else {
        Ok(())
    };
    // The local reference is a separately owned live peer. Release the exact
    // Java hold first, then cancel it immediately on phase/cleanup failure.
    let peer_cancellation = if failed_before_join {
        peer.as_deref()
            .map_or(Ok(()), PublicationSqlActor::cancel_active)
    } else {
        Ok(())
    };
    let outcome = outcome.unwrap_or_else(|| query.join());
    let unresolved_reader = match &outcome {
        Err(_) => true,
        Ok(Err(error)) => public_sql_error(error).is_err(),
        Ok(Ok(())) => false,
    };
    let peer_cancellation_after_join = if !failed_before_join && unresolved_reader {
        peer.as_deref()
            .map_or(Ok(()), PublicationSqlActor::cancel_active)
    } else {
        Ok(())
    };
    let peer_cancellation = both(peer_cancellation, peer_cancellation_after_join).map(|_| ());
    if let Some(peer) = peer {
        // A finished non-Trino peer still owns its exact pending capability.
        // Settle it before waiting for the independent original Java exit.
        peer.prejoin_finished();
    }
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
            "cancel_error":failure(&cancellation),"peer_cancel_error":failure(&peer_cancellation),"join_error":failure(&outcome),"sql_actor_actual_exit":true,"sql_actor_join_succeeded":query.join_succeeded,
            "sql_actor_settlement_succeeded":outcome.is_ok(),
            "sql_result":outcome.as_ref().ok().map(|result| format!("{result:?}")),
            "original_exit":format!("{exited:?}"),"trace_error":failure(&trace)
        }),
    );
    let cleanup = both(
        inspection,
        both(
            release,
            both(cancellation, both(peer_cancellation, both(trace, receipt))),
        ),
    )
    .map(|_| ());
    let phase = both(both(phase, exited), cleanup).map(|(value, ())| value);
    let ((phase, exited), outcome) = finish_query(phase, outcome)?;
    Ok((phase, outcome, exited))
}

fn late_commit(context: &mut ScenarioContext, fixture: &PublicationFixture) -> Result<()> {
    let before = metadata(fixture)?;
    let control = fixture.proxy()?;
    let counts = control.mutation_counts("ns", "orders");
    let mut hold = fixture.service().arm("ns.orders")?;
    let started = Instant::now();
    let query = service_sql(
        context,
        "service",
        &mut hold,
        format!("ALTER TABLE {TABLE} ADD COLUMN late_value BIGINT"),
    )?;
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
        None,
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
    let query = service_sql(
        context,
        "post-commit",
        &mut hold,
        "ALTER TABLE publication_response.ns.orders ADD COLUMN post_commit BIGINT".into(),
    )?;
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
        None,
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
        let cleanup = both(evidence, both(trace_saved, receipt));
        let evidence = match cleanup {
            Ok((evidence, _)) => evidence,
            Err(cleanup) => return both(outcome, Err::<(), _>(cleanup)).map(|_| ()),
        };
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
    let statement = if matches!(property, Property::ExpiryConcurrency) {
        format!(
            "ALTER TABLE {TABLE} EXPIRE SNAPSHOTS OLDER THAN '2099-01-01 00:00:00' RETAIN LAST 1"
        )
    } else {
        format!("ALTER TABLE {TABLE} REWRITE MANIFESTS")
    };
    let query = service_sql(context, "maintenance", &mut hold, statement)?;
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
        None,
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
    let actual = integer_rows(execute_sql(
        &mut new_session(context)?,
        format!("SELECT k FROM {TABLE} ORDER BY k"),
        context.deadline(),
    )?)?;
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
fn integer_rows(result: StatementResult) -> Result<Vec<i32>> {
    ensure!(
        result.is_result_set()
            && result.columns.len() == 1
            && result.columns[0].type_signature.raw_type == "integer"
            && result.columns[0].type_signature.arguments.is_empty(),
        "publication source rows lost their declared INT column"
    );
    result
        .rows
        .into_iter()
        .map(|row| match row.as_slice() {
            [TrinoValue::Integer(value)] => {
                i32::try_from(*value).context("publication INT row overflow")
            }
            actual => anyhow::bail!("publication source row requires one non-null INT: {actual:?}"),
        })
        .collect()
}
fn rows(context: &ScenarioContext, table: &str, version: Option<&str>) -> Result<Vec<i32>> {
    let version = version
        .map(|version| format!(" FOR VERSION AS OF {version}"))
        .unwrap_or_default();
    integer_rows(execute_sql(
        &mut new_session(context)?,
        format!("SELECT k FROM {CATALOG}.ns.{table}{version} ORDER BY k"),
        context.deadline(),
    )?)
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
    mut query: PublicationSqlActor,
) -> Result<(T, Result<()>)> {
    let release = guard.release();
    let cancellation = if phase.is_err() || release.is_err() {
        query.cancel_active()
    } else {
        Ok(())
    };
    let outcome = query.join();
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
            "cancel_error":failure(&cancellation),"join_error":failure(&outcome),"sql_actor_actual_exit":true,"sql_actor_join_succeeded":query.join_succeeded,
            "sql_actor_settlement_succeeded":outcome.is_ok(),
            "sql_result":outcome.as_ref().ok().map(|result| format!("{result:?}")),
            "proxy_actual_exit_error":failure(&exit),"trace_error":failure(&trace)
        }),
    );
    let cleanup = both(
        release,
        both(cancellation, both(exit, both(trace, receipt))),
    )
    .map(|_| ());
    finish_query(both(phase, cleanup).map(|(value, ())| value), outcome)
}

fn expiry_properties(
    context: &mut ScenarioContext,
    fixture: &PublicationFixture,
    observer: StatementObservationOnly,
) -> Result<()> {
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
        let (query, guard) = proxy_sql(context, case, guard, expire(CATALOG, &table))?;
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
    local_expiry_serialization(context, fixture, observer)
}
fn local_expiry_serialization(
    context: &mut ScenarioContext,
    fixture: &PublicationFixture,
    observer: StatementObservationOnly,
) -> Result<()> {
    let table = "expiry_local_serial";
    let before = seed(context, fixture, table, 2)?;
    let candidate = oldest(&before)?;
    let control = fixture.proxy()?;
    let initial_count = control.mutation_counts("ns", table).table_commit_forwarded;
    let mut hold = fixture.service().arm(&format!("ns.{table}"))?;
    let query = service_sql(
        context,
        "local-serialization",
        &mut hold,
        expire(CATALOG, table),
    )?;
    let mut reference = None;
    let mut reference_observation = None;
    let phase = (|| -> Result<()> {
        hold.wait_until_held(context.deadline())?;
        let count = control.mutation_counts("ns", table).table_commit_forwarded;
        let statement = format!(
            "ALTER TABLE {CATALOG}.ns.{table} CREATE TAG local_waiter AS OF VERSION {candidate}"
        );
        reference = Some(spawn_observed_sql(
            context,
            statement,
            observer,
            &mut reference_observation,
        )?);
        let observation = reference_observation
            .as_ref()
            .context("local reference nomination missing")?;
        let deadline = context.deadline();
        context
            .handle()
            .await_query_lifecycle_statement_owned(observation, deadline)?;
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
        reference.as_mut(),
        |_| Ok(()),
    );
    // Join the waiting ref even if release, the expiry actor or wrapper failed.
    let mut reference_join_succeeded = None;
    let reference = reference
        .map(|mut actor| {
            let cancellation = if expired.is_err() {
                actor.cancel_active()
            } else {
                Ok(())
            };
            let outcome = actor.join();
            reference_join_succeeded = actor.join_succeeded;
            finish_query(cancellation, outcome).map(|((), outcome)| outcome)
        })
        .transpose();
    // Both actual SQL actors have settled (or no ref reader started). Drain
    // only a successful original nomination, with the same absolute deadline.
    let binding_drain = match reference_observation.as_ref() {
        Some(observation) => {
            let deadline = context.deadline();
            context
                .handle()
                .drain_query_lifecycle_statement_binding(observation, deadline)
        }
        None => Ok(()),
    };
    let receipt = save(
        context,
        "local-serialization-ref.json",
        &json!({
            "expiry":format!("{expired:?}"),"ref_actor":format!("{reference:?}"),
            "ref_actor_actual_exit":reference_join_succeeded.is_some(),
            "ref_actor_join_succeeded":reference_join_succeeded,
            "observer_nominated":reference_observation.is_some(),
            "binding_drain_error":failure(&binding_drain)
        }),
    );
    let expired = both(expired, both(binding_drain, receipt)).map(|(expired, _)| expired);
    let reference = match expired {
        Ok(expired) => (expired, reference?),
        Err(primary) => {
            let peer = reference.and_then(|peer| peer.unwrap_or(Ok(())));
            return both(Err::<(), _>(primary), peer).map(|_| ());
        }
    };
    let ((_, expired, original), reference) = reference;
    expired.context("local serialized expiry did not apply")?;
    ensure!(
        original == CommitOutcome::Succeeded,
        "local expiry did not actually commit"
    );
    let rejected = reference
        .context("ref actor not started")?
        .err()
        .context("ref planned against an already expired snapshot after lock release")?;
    let diagnostic = &public_sql_error(&rejected)?.message;
    if !(diagnostic.to_ascii_lowercase().contains("snapshot")
        && diagnostic.contains(&candidate.to_string()))
    {
        return Err(
            rejected.context("local ref failed for a reason other than the removed snapshot")
        );
    }
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
    let query = service_sql(
        context,
        "unknown-window-service",
        &mut hold,
        expire("publication_unknown", table),
    )?;
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
        None,
        |outcome| {
            require_unknown(outcome)?;
            ensure!(
                public_sql_error(outcome.as_ref().unwrap_err())?
                    .message
                    .contains("[CommitUnknown]"),
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
    let (_, (_, (_, (after, ())))) =
        both(finished, both(retry, both(trace, both(after, receipt))))?;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn commit_unknown() -> StatementFailure {
        StatementFailure::Trino(serde_json::from_value(json!({
            "message":"[CommitUnknown] original publication remains Unknown",
            "errorCode":65536, "errorName":"GENERIC_INTERNAL_ERROR", "errorType":"INTERNAL_ERROR"
        })).unwrap())
    }

    #[test]
    fn publication_unknown_requires_original_trino_source() {
        assert!(require_unknown(&Err(commit_unknown().into())).is_ok());
        for error in [
            StatementFailure::Http {
                status: 503,
                body: "[CommitUnknown] Unknown".into(),
            },
            StatementFailure::Transport("Unknown publication transport".into()),
            StatementFailure::Deadline,
        ] {
            assert!(require_unknown(&Err(error.into())).is_err());
        }
    }

    #[test]
    fn publication_cleanup_preserves_original_sql_source() {
        let outcome = finish_query::<()>(
            Err(anyhow::anyhow!("original hold release failed")),
            Ok(Err(commit_unknown().into())),
        )
        .unwrap_err();
        let wire = public_sql_error(&outcome).unwrap();
        assert_eq!(wire.error_code, 65536);
        assert!(wire.message.starts_with("[CommitUnknown]"));
        assert!(format!("{outcome:#}").contains("original hold release failed"));
    }
}
