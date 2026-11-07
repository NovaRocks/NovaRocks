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

//! Native Trino protocol correctness with an independent HTTP/JSON oracle.
//!
//! These default System assets protect facts not covered by MySQL actors:
//! page acknowledgement/replay, explicit client session changes, statement
//! cancellation, tombstone lifetime and drain across a real FE/BE boundary.
//! Every case requires a fresh 1FE+3BE harness. Inputs are finite constants in
//! this module; no Trino image or external shared endpoint is used. The Trino
//! entrypoint owner maintains these cases with the default functional suite.
use super::result_delivery_support::{await_idle, metric};
use crate::{
    actors::trino_protocol::{RawResponse, TrinoClient},
    scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig},
};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::{
    CrossProcessConfigOverlay, TestClusterOwner,
    resource_registration::{self, Registration},
};
use reqwest::Method;
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Mutex,
    thread,
    time::{Duration, Instant},
};

const PAGING_ROWS: usize = 64;
const PAYLOAD_BYTES: usize = 65_536;
const ABANDON_MS: u64 = 2_500;
const POLL: Duration = Duration::from_millis(25);

pub(super) fn scenarios() -> Vec<Box<dyn Scenario>> {
    [
        Case::Paging,
        Case::Interrupted,
        Case::Session,
        Case::Delete,
        Case::Abandon,
        Case::Drain,
        Case::Refusals,
        Case::RowBudget,
        Case::UpdateType,
        Case::Completion,
        Case::StatementSize,
        Case::Info,
    ]
    .into_iter()
    .map(|case| {
        Box::new(TrinoProtocol {
            case,
            resource: Mutex::new(None),
        }) as Box<dyn Scenario>
    })
    .collect()
}
#[derive(Clone, Copy)]
enum Case {
    Paging,
    Interrupted,
    Session,
    Delete,
    Abandon,
    Drain,
    Refusals,
    RowBudget,
    UpdateType,
    Completion,
    StatementSize,
    Info,
}
struct TrinoProtocol {
    case: Case,
    resource: Mutex<Option<WarehouseResource>>,
}
struct WarehouseResource {
    path: PathBuf,
    registration: Registration,
}
impl Scenario for TrinoProtocol {
    fn name(&self) -> &'static str {
        match self.case {
            Case::Paging => "trino-protocol/paging-ack-and-replay",
            Case::Interrupted => "trino-protocol/interrupted-response-retry",
            Case::Session => "trino-protocol/session-effects",
            Case::Delete => "trino-protocol/delete-cancels",
            Case::Abandon => "trino-protocol/abandon-releases",
            Case::Drain => "trino-protocol/drain",
            Case::Refusals => "trino-protocol/refusals",
            Case::RowBudget => "trino-protocol/row-exceeds-page-budget",
            Case::UpdateType => "trino-protocol/ddl-update-type",
            Case::Completion => "trino-protocol/command-completion-shape",
            Case::StatementSize => "trino-protocol/statement-too-large",
            Case::Info => "trino-protocol/info-and-head",
        }
    }
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module("trino_protocol", self.name(), false, false)
    }
    fn launch_config(&self, _: &Path) -> Result<ScenarioLaunchConfig> {
        let fe = match self.case {
            Case::Abandon => Some(format!(
                "[standalone_server]\ntrino_client_timeout_ms = {ABANDON_MS}\n"
            )),
            _ => None,
        };
        Ok(ScenarioLaunchConfig {
            config_overlay: CrossProcessConfigOverlay {
                fe,
                ..Default::default()
            },
            ..Default::default()
        })
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "Trino protocol cases require native 1FE+3BE"
        );
        context.action("verified native 1FE+3BE topology for Trino protocol correctness");
        let epoch = Instant::now();
        await_idle(context, "trino", "before", epoch)?;
        let actor = TrinoClient::new(
            context.trino_port(),
            context.sql_user(),
            context.deadline(),
            &context.scenario_root().join("trino-http"),
        )?;
        let outcome = match self.case {
            Case::Paging => paging(context, &actor),
            Case::Interrupted => interrupted(context, &actor),
            Case::Session => {
                self.prepare_catalog(context, &actor)?;
                session_effects(context, &actor)
            }
            Case::Delete => cancel(context, &actor),
            Case::Abandon => abandon(context, &actor),
            Case::Drain => drain(context, &actor),
            Case::Refusals => refusals(context, &actor),
            Case::RowBudget => row_budget(context, &actor),
            Case::UpdateType => {
                self.prepare_catalog(context, &actor)?;
                update_types(context, &actor)
            }
            Case::Completion => {
                self.prepare_catalog(context, &actor)?;
                command_completion(context, &actor)
            }
            Case::StatementSize => statement_size(context, &actor),
            Case::Info => info(context, &actor),
        };
        if matches!(
            self.case,
            Case::Session | Case::UpdateType | Case::Completion
        ) {
            let cleanup = success(&actor, "DROP CATALOG IF EXISTS trc1_catalog");
            match (outcome, cleanup) {
                (Err(primary), Err(cleanup)) => {
                    anyhow::bail!("{primary:#}; Trino catalog cleanup: {cleanup:#}")
                }
                (Err(error), _) | (_, Err(error)) => return Err(error),
                (Ok(()), Ok(_)) => context.action(
                    "removed Trino-created catalog attachment through the Trino entrypoint",
                ),
            }
        } else {
            outcome?;
        }
        if !matches!(self.case, Case::Drain) {
            await_idle(context, "trino", "after", epoch)?;
        }
        Ok(())
    }
    fn teardown(&self) -> Result<()> {
        let mut resource = self
            .resource
            .lock()
            .map_err(|_| anyhow::anyhow!("Trino warehouse cleanup lock poisoned"))?
            .take();
        if let Some(resource) = resource.as_mut() {
            // Runner invokes scenario teardown after actual cluster shutdown.
            // This exact, registered local warehouse is the only deletion scope.
            let cleanup = match fs::remove_dir_all(&resource.path) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => {
                    Err(anyhow::Error::from(error).context("remove owned Trino warehouse"))
                }
            };
            let recorded = resource.registration.settle(&cleanup);
            match (cleanup, recorded) {
                (Err(primary), Err(record)) => {
                    anyhow::bail!("{primary:#}; warehouse cleanup reporting: {record:#}")
                }
                (Err(error), _) | (_, Err(error)) => return Err(error),
                (Ok(()), Ok(())) => {}
            }
        }
        Ok(())
    }
}
impl TrinoProtocol {
    fn prepare_catalog(&self, context: &mut ScenarioContext, actor: &TrinoClient) -> Result<()> {
        let warehouse = context.scenario_root().join("trino-owned-warehouse");
        let registration = resource_registration::register(
            context.scenario_root(),
            "trino-protocol",
            "warehouse",
            &format!(
                "Trino scenario local Iceberg warehouse {}",
                warehouse.display()
            ),
        )?;
        *self
            .resource
            .lock()
            .map_err(|_| anyhow::anyhow!("Trino warehouse cleanup lock poisoned"))? =
            Some(WarehouseResource {
                path: warehouse.clone(),
                registration,
            });
        fs::create_dir_all(&warehouse)?;
        let warehouse = warehouse.to_string_lossy().replace('"', "\\\"");
        success(
            actor,
            &format!(
                "CREATE EXTERNAL CATALOG trc1_catalog PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"hadoop\",\"iceberg.catalog.warehouse\"=\"{warehouse}\")"
            ),
        )?;
        success(actor, "CREATE DATABASE trc1_catalog.first")?;
        success(actor, "CREATE DATABASE trc1_catalog.second")?;
        success(actor, "CREATE TABLE trc1_catalog.first.items (id INT)")?;
        success(
            actor,
            "INSERT INTO trc1_catalog.first.items VALUES (7), (11)",
        )?;
        context.action(
            "created registered local Iceberg catalog, schemas and table through Trino DDL/DML",
        );
        Ok(())
    }
}
fn terminal(pages: &[RawResponse]) -> Result<&RawResponse> {
    let terminal = pages.last().context("query omitted all responses")?;
    ensure!(
        terminal.next_uri()?.is_none(),
        "query did not reach a terminal page"
    );
    Ok(terminal)
}
fn success(actor: &TrinoClient, sql: &str) -> Result<Vec<RawResponse>> {
    let pages = actor.execute(sql)?;
    for page in &pages {
        page.assert_success()?;
    }
    let final_json = terminal(&pages)?.json()?;
    ensure!(
        final_json["stats"]["state"] == "FINISHED",
        "query did not finish: {final_json}"
    );
    ensure!(
        final_json.get("data").is_none(),
        "terminal success page contains data"
    );
    Ok(pages)
}
pub(super) fn require_error(response: &RawResponse, expected: (i64, &str, &str)) -> Result<Value> {
    ensure!(
        response.status == 200,
        "expected a Trino QueryError, got HTTP {}",
        response.status
    );
    let value = response.json()?;
    ensure!(
        value["error"]["errorCode"].as_i64() == Some(expected.0)
            && value["error"]["errorName"].as_str() == Some(expected.1)
            && value["error"]["errorType"].as_str() == Some(expected.2),
        "expected QueryError {expected:?}, got {value}"
    );
    ensure!(
        value.get("nextUri").is_none(),
        "error page contains nextUri"
    );
    for field in ["columns", "data", "updateType", "updateCount"] {
        ensure!(
            value.get(field).is_none(),
            "error page contains success field {field}"
        );
    }
    Ok(value)
}
fn failed_query(actor: &TrinoClient, sql: &str, expected: (i64, &str, &str)) -> Result<Value> {
    let pages = actor.execute(sql)?;
    require_error(terminal(&pages)?, expected)
}
fn paging_sql() -> String {
    format!(
        "SELECT generate_series AS id, REPEAT('x', {PAYLOAD_BYTES}) AS payload FROM generate_series(1, {PAGING_ROWS}) ORDER BY generate_series"
    )
}
fn assert_row(row: &Value, expected: usize) -> Result<()> {
    let values = row
        .as_array()
        .context("Trino row is not a complete JSON array")?;
    ensure!(values.len() == 2, "Trino row width changed");
    ensure!(
        values[0].as_u64() == Some(expected as u64),
        "row order/content mismatch: {row}"
    );
    let text = values[1]
        .as_str()
        .context("Trino payload is not a string")?;
    ensure!(
        text.len() == PAYLOAD_BYTES && text.bytes().all(|byte| byte == b'x'),
        "Trino row payload changed"
    );
    Ok(())
}
fn paging(context: &mut ScenarioContext, actor: &TrinoClient) -> Result<()> {
    let before = observation(context, "paging-before")?;
    ensure!(before.active == 0, "Trino registry was active before query");
    let post = actor.submit(&paging_sql())?;
    post.assert_success()?;
    let mut uri = post.next_uri()?.context("POST omitted initial nextUri")?;
    let initial = uri.clone();
    let mut expected = 1usize;
    let mut data_pages = 0;
    let mut last_data_had_next = false;
    loop {
        let page = actor.get(&uri)?;
        let value = page.assert_success()?;
        let replay = actor.get(&uri)?;
        ensure!(
            replay.status == page.status && replay.body == page.body,
            "same-token replay changed original response bytes"
        );
        ensure!(
            replay.semantic_headers() == page.semantic_headers(),
            "same-token replay changed response headers"
        );
        let next = page.next_uri()?;
        if let Some(rows) = value.get("data") {
            let rows = rows.as_array().context("Trino data is not an array")?;
            ensure!(!rows.is_empty(), "data page has no complete rows");
            for row in rows {
                assert_row(row, expected)?;
                expected += 1;
            }
            data_pages += 1;
            last_data_had_next = next.is_some();
            ensure!(
                last_data_had_next,
                "last data page omitted its acknowledgement URI"
            );
            if expected == PAGING_ROWS + 1 {
                let pending = observation(context, "paging-before-terminal-ack")?;
                ensure!(
                    pending.active == 1,
                    "last data page prematurely settled the Trino query: {pending:?}"
                );
                ensure!(
                    pending.complete == before.complete,
                    "completion happened before terminal acknowledgement"
                );
                require_original_statement_held(context, "paging-before-terminal-ack")?;
            }
        }
        match next {
            Some(next) => uri = next,
            None => {
                ensure!(
                    value["stats"]["state"] == "FINISHED" && value.get("data").is_none(),
                    "invalid terminal page: {value}"
                );
                let replay = actor.get(&uri)?;
                ensure!(
                    replay.status == page.status && replay.body == page.body,
                    "terminal tombstone replay changed bytes"
                );
                break;
            }
        }
    }
    ensure!(
        expected == PAGING_ROWS + 1 && data_pages >= 2 && last_data_had_next,
        "multi-page independent row oracle was not covered"
    );
    ensure!(
        actor.get(&initial)?.status == 410,
        "old token did not return 410"
    );
    let unknown = uri
        .rsplit_once('/')
        .map(|(prefix, _)| format!("{prefix}/18446744073709551615"))
        .context("terminal URI has no token")?;
    ensure!(
        actor.get(&unknown)?.status == 404,
        "unknown token did not return 404"
    );
    let wrong_slug = uri.replacen("/executing/", "/executing/unknown-", 1);
    ensure!(
        actor.get(&wrong_slug)?.status == 404,
        "unknown query URI did not return 404"
    );
    let after = await_observation(context, "paging-after", |snapshot| snapshot.active == 0)?;
    ensure!(
        after.complete == before.complete + 1,
        "query did not settle complete exactly once"
    );
    context.action("verified complete ordered rows, multi-page final ACK, exact replay, tombstone replay, 410/404 and single settlement");
    Ok(())
}
fn interrupted(context: &mut ScenarioContext, actor: &TrinoClient) -> Result<()> {
    let post = actor.submit(&paging_sql())?;
    let mut uri = post.next_uri()?.context("POST omitted initial URI")?;
    loop {
        let page = actor.get(&uri)?;
        let value = page.assert_success()?;
        if value.get("data").is_some() {
            let mut held = actor.open_get(&uri)?;
            let at_headers = held.observe_held()?;
            ensure!(
                at_headers.read_calls == 0
                    && !at_headers.first_byte_observed
                    && !at_headers.eof_observed,
                "headers were confused with a first body read or EOF"
            );
            let partial = RawResponse {
                status: held.status(),
                headers: held.headers().clone(),
                body: held.read_prefix(128)?,
            };
            let prefix = held.observe_held()?;
            ensure!(
                prefix.first_byte_observed && !prefix.eof_observed && prefix.read_bytes == 128,
                "original strict prefix was not held before EOF"
            );
            let joined = held.interrupt();
            joined.require_clean()?;
            ensure!(
                !joined.observation.eof_observed && joined.observation.read_bytes == 128,
                "interruption drained the response or replaced the original prefix"
            );
            ensure!(
                partial.status == page.status
                    && partial.semantic_headers() == page.semantic_headers(),
                "interrupted original response changed status or repeated headers"
            );
            ensure!(
                page.body.starts_with(&partial.body) && partial.body.len() < page.body.len(),
                "interrupted response did not preserve a strict body prefix"
            );
            let replay = actor.get(&uri)?;
            ensure!(
                replay.status == page.status
                    && replay.body == page.body
                    && replay.semantic_headers() == page.semantic_headers(),
                "interrupted response changed replay bytes/headers"
            );
            while let Some(next) = replay_next(actor, &uri)? {
                uri = next;
            }
            break;
        }
        uri = page
            .next_uri()?
            .context("query ended before interrupted data page")?;
    }
    context.action("held original HTTP headers, observed a strict128-byte prefix before EOF, actually dropped/joined that reader, replayed identical full bytes/headers, and completed the same query");
    Ok(())
}
fn replay_next(actor: &TrinoClient, uri: &str) -> Result<Option<String>> {
    let page = actor.get(uri)?;
    let value = page.assert_success()?;
    let next = page.next_uri()?;
    if next.is_none() {
        ensure!(
            value["stats"]["state"] == "FINISHED",
            "interrupted query did not finish"
        );
    }
    Ok(next)
}
fn session_effects(context: &mut ScenarioContext, actor: &TrinoClient) -> Result<()> {
    let client = actor.with_session(&[
        ("x-trino-catalog", "trc1_catalog"),
        ("x-trino-schema", "first"),
    ]);
    let selected = success(&client, "SELECT id FROM items ORDER BY id")?;
    let rows: Vec<_> = selected
        .iter()
        .map(RawResponse::json)
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .filter_map(|page| page.get("data").cloned())
        .flat_map(|data| data.as_array().cloned().unwrap_or_default())
        .collect();
    ensure!(
        rows == vec![json!([7]), json!([11])],
        "catalog/schema headers did not resolve unqualified table: {rows:?}"
    );
    let mut stateful = client.clone();
    let pages = success(&stateful, "USE trc1_catalog.second")?;
    let end = terminal(&pages)?;
    ensure!(
        end.headers
            .get("x-trino-set-catalog")
            .context("USE omitted Set-Catalog")?
            == "trc1_catalog",
        "USE returned wrong catalog"
    );
    ensure!(
        end.headers
            .get("x-trino-set-schema")
            .context("USE omitted Set-Schema")?
            == "second",
        "USE returned wrong schema"
    );
    for header in ["x-trino-set-session", "x-trino-clear-session"] {
        ensure!(
            !end.headers.contains_key(header),
            "qualified USE emitted unexpected session effect {header}"
        );
    }
    stateful.apply_effects(end)?;
    success(&stateful, "DROP DATABASE trc1_catalog.second")?;
    let pages = success(&stateful, "USE trc1_catalog.first")?;
    ensure!(
        terminal(&pages)?
            .headers
            .get("x-trino-set-schema")
            .context("USE after stale schema omitted effect")?
            == "first",
        "USE failed to replace dropped current schema"
    );
    for (sql, expected_effect) in [
        ("SET SESSION pipeline_dop = 1 + 2", "pipeline_dop=3"),
        (
            "SET SESSION global_runtime_filter_probe_min_selectivity = 1",
            "global_runtime_filter_probe_min_selectivity=1.0",
        ),
    ] {
        let pages = success(actor, sql)?;
        let end = terminal(&pages)?;
        let effects = end
            .headers
            .get_all("x-trino-set-session")
            .iter()
            .map(|value| value.to_str().context("Set-Session effect is not text"))
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            effects == vec![expected_effect],
            "SET returned wrong evaluated effect: expected {expected_effect}, got {effects:?}"
        );
        for header in [
            "x-trino-set-catalog",
            "x-trino-set-schema",
            "x-trino-clear-session",
        ] {
            ensure!(
                !end.headers.contains_key(header),
                "SET emitted unrelated effect {header}"
            );
        }
    }
    let pages = success(actor, "SET query_timeout = 1")?;
    let end = terminal(&pages)?;
    ensure!(
        end.headers
            .get_all("x-trino-set-session")
            .iter()
            .any(|value| value == "query_timeout=1"),
        "SET omitted typed Set-Session effect"
    );
    let mut timed = actor.clone();
    timed.apply_effects(end)?;
    failed_query(
        &timed,
        "SELECT sleep(3)",
        (131075, "EXCEEDED_TIME_LIMIT", "INSUFFICIENT_RESOURCES"),
    )?;
    failed_query(
        &actor.with_session(&[("x-trino-session", "trc_unknown_property=1")]),
        "SELECT 1",
        (14, "INVALID_SESSION_PROPERTY", "USER_ERROR"),
    )?;
    for (raw, expected) in [
        ("trc_nocatalog.x=1", (44, "CATALOG_NOT_FOUND", "USER_ERROR")),
        (
            "trc1_catalog.x=1",
            (14, "INVALID_SESSION_PROPERTY", "USER_ERROR"),
        ),
        (
            "PIPELINE_DOP=1",
            (14, "INVALID_SESSION_PROPERTY", "USER_ERROR"),
        ),
        (
            "catalog=trc1_catalog",
            (14, "INVALID_SESSION_PROPERTY", "USER_ERROR"),
        ),
        (
            "enable_global_runtime_filter=yes",
            (14, "INVALID_SESSION_PROPERTY", "USER_ERROR"),
        ),
        (
            "pipeline_dop=abc",
            (14, "INVALID_SESSION_PROPERTY", "USER_ERROR"),
        ),
        (
            "trc_nocatalog.x=1,trc_unknown_property=1",
            (14, "INVALID_SESSION_PROPERTY", "USER_ERROR"),
        ),
        (
            "trc_unknown_property=1,trc_nocatalog.x=1",
            (14, "INVALID_SESSION_PROPERTY", "USER_ERROR"),
        ),
    ] {
        let request = actor.with_session(&[("x-trino-session", raw)]);
        let pages = request.execute("SELECT 1")?;
        require_error(terminal(&pages)?, expected)?;
        for page in &pages {
            for header in [
                "x-trino-set-session",
                "x-trino-clear-session",
                "x-trino-set-catalog",
                "x-trino-set-schema",
            ] {
                ensure!(
                    !page.headers.contains_key(header),
                    "refused session header {raw} emitted effect {header}"
                );
            }
        }
    }
    failed_query(
        &actor.with_session(&[("x-trino-time-zone", "Mars/Olympus")]),
        "SELECT 1",
        (13, "NOT_SUPPORTED", "USER_ERROR"),
    )?;
    let only_schema = actor.request(
        Method::POST,
        &format!("{}/v1/statement", actor.base()),
        Some("SELECT 1"),
        &[("x-trino-schema", "first")],
    )?;
    ensure!(
        only_schema.status == 400,
        "schema without catalog did not fail during POST syntax validation"
    );
    context.action("verified name headers, qualified USE without session clearing, evaluated SET effects, exact double rendering, typed 14/44 refusals, strict header decoding and system-before-catalog validation in both mixed orders");
    Ok(())
}
fn cancel(context: &mut ScenarioContext, actor: &TrinoClient) -> Result<()> {
    let before = observation(context, "delete-before")?;
    let post = actor.submit("SELECT sleep(60)")?;
    let initial = post.next_uri()?.context("POST omitted nextUri")?;
    let running = actor.get(&initial)?;
    running.assert_success()?;
    let uri = running
        .next_uri()?
        .context("long query ended before DELETE")?;
    ensure!(
        observation(context, "delete-running")?.active == 1,
        "DELETE input never entered active execution"
    );
    require_original_statement_held(context, "delete-running")?;
    ensure!(
        actor.delete(&uri)?.status == 204,
        "DELETE did not return 204"
    );
    require_error(&actor.get(&uri)?, (3, "USER_CANCELED", "USER_ERROR"))?;
    ensure!(
        actor.delete(&uri)?.status == 204,
        "repeated DELETE is not idempotent"
    );
    require_error(&actor.get(&uri)?, (3, "USER_CANCELED", "USER_ERROR"))?;
    let after = await_observation(context, "delete-after", |snapshot| snapshot.active == 0)?;
    ensure!(
        after.client_cancel == before.client_cancel + 1,
        "DELETE did not settle exactly once: {before:?} -> {after:?}"
    );
    context.action(
        "verified active DELETE, repeated cancellation, USER_CANCELED tombstone and one settlement",
    );
    Ok(())
}
fn abandon(context: &mut ScenarioContext, actor: &TrinoClient) -> Result<()> {
    let before = observation(context, "abandon-before")?;
    let queued = actor.submit("SELECT 1")?;
    let queued_uri = queued.next_uri()?.context("POST omitted registered URI")?;
    thread::sleep(Duration::from_millis(ABANDON_MS + 300));
    ensure!(
        actor.get(&queued_uri)?.status == 404,
        "POST-only query was not removed after abandonment"
    );
    ensure!(
        observation(context, "abandon-post-only")?.active == 0,
        "POST-only query acquired execution ownership"
    );
    let post = actor.submit("SELECT sleep(60)")?;
    let running = actor.get(&post.next_uri()?.context("POST omitted initial URI")?)?;
    let uri = running
        .next_uri()?
        .context("abandon input ended before polling stopped")?;
    ensure!(
        observation(context, "abandon-running")?.active == 1,
        "abandon input did not enter execution"
    );
    require_original_statement_held(context, "abandon-running")?;
    thread::sleep(Duration::from_millis(ABANDON_MS + 300));
    let result = actor.get(&uri)?;
    if result.status != 404 {
        require_error(&result, (2, "ABANDONED_QUERY", "USER_ERROR"))?;
    }
    let after = await_observation(context, "abandon-after", |snapshot| snapshot.active == 0)?;
    ensure!(
        after.abandoned == before.abandoned + 1,
        "executing abandonment did not settle once"
    );
    context
        .action("verified POST-only expiry and executing-client abandonment release independently");
    Ok(())
}
fn drain(context: &mut ScenarioContext, actor: &TrinoClient) -> Result<()> {
    let post = actor.submit("SELECT sleep(5)")?;
    let running = actor.get(&post.next_uri()?.context("POST omitted initial URI")?)?;
    let mut uri = running
        .next_uri()?
        .context("drain input ended before drain")?;
    ensure!(
        observation(context, "drain-running")?.active == 1,
        "drain input did not enter execution"
    );
    context.handle().begin_fe_drain()?;
    loop {
        context.remaining("observe Trino FE drain")?;
        let state = context
            .handle()
            .frontend_management_get("/readyz", Duration::from_secs(1))?;
        if state.status == 503 {
            break;
        }
        thread::sleep(POLL);
    }
    let refusal = actor.submit("SELECT 1")?;
    require_error(&refusal, (65545, "SERVER_SHUTTING_DOWN", "INTERNAL_ERROR"))?;
    loop {
        let page = actor.get(&uri)?;
        let value = page.assert_success()?;
        if let Some(next) = page.next_uri()? {
            uri = next;
        } else {
            ensure!(
                value["stats"]["state"] == "FINISHED",
                "pre-drain query failed: {value}"
            );
            break;
        }
    }
    let deadline = context.deadline();
    context.handle().wait_fe_exit_until(deadline)?;
    context.action("rejected new Trino POST during drain, finished admitted query, and observed successful actual FE exit");
    Ok(())
}
fn refusals(context: &mut ScenarioContext, actor: &TrinoClient) -> Result<()> {
    let submit = format!("{}/v1/statement", actor.base());
    ensure!(
        actor
            .request(
                Method::POST,
                &submit,
                Some("SELECT 1"),
                &[("host", "evil.example")]
            )?
            .status
            == 403,
        "foreign Host was accepted"
    );
    ensure!(
        actor
            .request(
                Method::POST,
                &submit,
                Some("SELECT 1"),
                &[("x-trino-user", "wrong-user")]
            )?
            .status
            == 401,
        "mismatched user was accepted"
    );
    for (name, value) in [
        ("x-trino-transaction-id", "123"),
        ("x-trino-prepared-statement", "prepared=SELECT+1"),
        ("x-trino-role", "system=ALL"),
        ("x-trino-extra-credential", "key=value"),
        ("x-trino-path", "catalog.schema"),
    ] {
        failed_query(
            &actor.with_session(&[(name, value)]),
            "SELECT 1",
            (13, "NOT_SUPPORTED", "USER_ERROR"),
        )?;
    }
    let error = failed_query(
        actor,
        "SELECT CAST(1 AS LARGEINT) AS wide_value",
        (13, "NOT_SUPPORTED", "USER_ERROR"),
    )?;
    ensure!(
        error["error"]["message"]
            .as_str()
            .context("missing refusal message")?
            .contains("wide_value"),
        "unsupported type error omitted actual column name"
    );
    context.action("verified Host/user HTTP refusals, unsupported feature errors and named LARGEINT type rejection");
    Ok(())
}
fn row_budget(context: &mut ScenarioContext, actor: &TrinoClient) -> Result<()> {
    let before = observation(context, "row-budget-before")?;
    // Each legal scalar remains at the existing 1 MiB string boundary; seven
    // fields form one row larger than the Client page remainder below 6 MiB.
    let fields = (0..7)
        .map(|index| format!("REPEAT('x', 1048576) AS field_{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    failed_query(
        actor,
        &format!("SELECT {fields}"),
        (
            131072,
            "GENERIC_INSUFFICIENT_RESOURCES",
            "INSUFFICIENT_RESOURCES",
        ),
    )?;
    let after = await_observation(context, "row-budget-after", |snapshot| snapshot.active == 0)?;
    ensure!(
        after.failed == before.failed + 1,
        "oversized row did not settle failure exactly once"
    );
    context.action("rejected one independently sized seven-field row above page budget with one resource settlement");
    Ok(())
}
fn update_types(context: &mut ScenarioContext, actor: &TrinoClient) -> Result<()> {
    for (sql, expected) in [
        (
            "CREATE TABLE trc1_catalog.first.update_type (id INT)",
            "CREATE TABLE",
        ),
        ("DROP TABLE trc1_catalog.first.update_type", "DROP TABLE"),
        ("SET query_timeout = 60", "SET SESSION"),
    ] {
        assert_raw_completion(actor, sql, expected, RawCompletionExpectation::Empty)?;
    }
    context.action("verified DDL and SET updateType names with no row columns");
    Ok(())
}

/// Independent scenario expectations, never derived from a returned counter,
/// SQL text, writer metric or adapter type. CTAS's explicit original fixture
/// calls this same raw oracle with its own finite source-row formula.
#[derive(Clone, Copy)]
pub(in crate::scenarios) enum RawCompletionExpectation {
    Empty,
    Counted(u64),
    NoCount,
}

pub(in crate::scenarios) fn assert_raw_completion(
    actor: &TrinoClient,
    sql: &str,
    update_type: &str,
    expected: RawCompletionExpectation,
) -> Result<String> {
    let post = actor.submit(sql)?;
    let mut cleanup_uri = post.next_uri().ok().flatten();
    let phase = (|| -> Result<String> {
        let initial = post.assert_success()?;
        let query_id = initial["id"]
            .as_str()
            .filter(|id| !id.is_empty())
            .context("POST omitted query identity")?
            .to_owned();
        ensure!(
            initial["stats"]["state"] == "QUEUED",
            "POST did not register a queued query"
        );
        for field in ["updateType", "columns", "data", "updateCount"] {
            ensure!(
                initial.get(field).is_none(),
                "pre-admission POST exposed {field}"
            );
        }
        let mut uri = post
            .next_uri()?
            .context("POST omitted original continuation")?;
        let columns = match expected {
            RawCompletionExpectation::Empty => json!([]),
            RawCompletionExpectation::Counted(_) | RawCompletionExpectation::NoCount => {
                json!([{"name":"rows","type":"bigint","typeSignature":{"rawType":"bigint","arguments":[]}}])
            }
        };
        let mut admitted = false;
        let mut data_seen = false;
        loop {
            // Only this current page and its same-token replay are in memory;
            // no complete-query Vec<RawResponse> is retained by this oracle.
            let page = actor.get(&uri)?;
            cleanup_uri = Some(uri.clone());
            let value = page.assert_success()?;
            ensure!(
                value["id"].as_str() == Some(query_id.as_str()),
                "completion query identity changed"
            );
            let replay = actor.get(&uri)?;
            ensure!(
                page.status == replay.status
                    && page.body == replay.body
                    && page.semantic_headers() == replay.semantic_headers(),
                "completion same-token replay changed bytes or repeated headers"
            );
            let next = page.next_uri()?;
            // The wire cannot expose the internal admission timestamp. Before
            // its first visible metadata only the entirely absent shape is
            // legal; adapter gated tests prove immediate actual-admission
            // publication. Once visible, both static fields must persist.
            let any_completion_field = ["updateType", "columns", "data", "updateCount"]
                .iter()
                .any(|name| value.get(*name).is_some());
            if !admitted && !any_completion_field {
                ensure!(next.is_some(), "success ended before observable admission");
            } else {
                admitted = true;
                ensure!(
                    value["updateType"].as_str() == Some(update_type),
                    "completion updateType changed or disappeared: {value}"
                );
                ensure!(
                    value.get("columns") == Some(&columns),
                    "completion static columns changed or disappeared: {value}"
                );
                match expected {
                    RawCompletionExpectation::Empty => {
                        ensure!(
                            value.get("data").is_none() && value.get("updateCount").is_none(),
                            "Empty completion exposed data or a fabricated count"
                        );
                    }
                    RawCompletionExpectation::Counted(count) => {
                        if let Some(data) = value.get("data") {
                            ensure!(
                                !data_seen && *data == json!([[count]]),
                                "count data must be exactly one independently expected bigint cell"
                            );
                            ensure!(next.is_some(), "count data omitted its acknowledgement URI");
                            data_seen = true;
                        }
                        if data_seen {
                            ensure!(
                                value.get("updateCount") == Some(&json!(count)),
                                "actual count did not persist from its first data page"
                            );
                        } else {
                            ensure!(
                                value.get("updateCount").is_none(),
                                "updateCount appeared before the actual count data page"
                            );
                        }
                    }
                    RawCompletionExpectation::NoCount => {
                        ensure!(
                            value.get("updateCount").is_none(),
                            "NoCount fabricated an updateCount"
                        );
                        if let Some(data) = value.get("data") {
                            ensure!(
                                !data_seen && *data == json!([[null]]),
                                "NoCount data must be exactly one NULL cell, never zero"
                            );
                            ensure!(
                                next.is_some(),
                                "NULL count data omitted its acknowledgement URI"
                            );
                            data_seen = true;
                        }
                    }
                }
            }
            if let Some(next) = next {
                uri = next;
                continue;
            }
            ensure!(
                admitted && value["stats"]["state"] == "FINISHED",
                "completion omitted admission or actual terminal success"
            );
            ensure!(
                value.get("data").is_none(),
                "terminal success still contains data"
            );
            match expected {
                RawCompletionExpectation::Empty => ensure!(!data_seen, "Empty produced a row"),
                RawCompletionExpectation::Counted(_) | RawCompletionExpectation::NoCount => {
                    ensure!(data_seen, "rows-bigint completion omitted its one data row")
                }
            }
            return Ok(query_id.clone());
        }
    })();
    finish_raw_registered(actor, cleanup_uri, phase)
}

fn command_completion(context: &mut ScenarioContext, actor: &TrinoClient) -> Result<()> {
    use RawCompletionExpectation::{Counted, Empty, NoCount};
    // Same original local Hadoop warehouse owner as the existing session/DDL
    // cases. Format3 row lineage is the already supported P01 DML input.
    for (sql, update_type, expected) in [
        (
            "CREATE TABLE trc1_catalog.first.completion_items (id INT, v INT) TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")",
            "CREATE TABLE",
            Empty,
        ),
        (
            "INSERT INTO trc1_catalog.first.completion_items VALUES (1, 10), (2, 20)",
            "INSERT",
            Counted(2),
        ),
        (
            "UPDATE trc1_catalog.first.completion_items SET v = v + 1 WHERE id = 1",
            "UPDATE",
            Counted(1),
        ),
        (
            "DELETE FROM trc1_catalog.first.completion_items WHERE id = 2",
            "DELETE",
            Counted(1),
        ),
        (
            "MERGE INTO trc1_catalog.first.completion_items t USING (SELECT 1 AS id, 100 AS v UNION ALL SELECT 3, 300) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)",
            "MERGE",
            Counted(2),
        ),
        (
            "UPDATE trc1_catalog.first.completion_items SET v = 0 WHERE id = 99",
            "UPDATE",
            Counted(0),
        ),
        // Hadoop's StatementJob admission is AwaitTerminal. The supported
        // plain ANALYZE waits for actual executor exit, then JobSubmitted is
        // genuinely NoCount. WITH SYNC MODE/FULL are currently unsupported.
        (
            "ANALYZE TABLE trc1_catalog.first.completion_items",
            "ANALYZE",
            NoCount,
        ),
    ] {
        let query_id = assert_raw_completion(actor, sql, update_type, expected)?;
        context.action(format!(
            "raw completion shape/replay verified client_query_id={query_id}"
        ));
    }
    let rows = success(
        actor,
        "SELECT id, v FROM trc1_catalog.first.completion_items ORDER BY id",
    )?;
    let mut actual = Vec::new();
    for page in rows {
        if let Some(data) = page.json()?.get("data").and_then(Value::as_array) {
            actual.extend(data.iter().cloned());
        }
    }
    ensure!(
        actual == vec![json!([1, 100]), json!([3, 300])],
        "ordinary DML changed independently expected surviving rows"
    );
    context.action("raw Empty/INSERT2/UPDATE1/DELETE1/MERGE2/UPDATE0/ANALYZE-NULL passed; raw CTAS remains mandatory in the original explicit fenced REST supplement");
    Ok(())
}

fn finish_raw_registered<T>(
    actor: &TrinoClient,
    cleanup_uri: Option<String>,
    result: Result<T>,
) -> Result<T> {
    match result {
        Ok(value) => Ok(value),
        Err(primary) => match cleanup_uri {
            Some(uri) => match actor.delete(&uri) {
                Ok(response) if response.status == 204 => Err(primary),
                Ok(response) => anyhow::bail!(
                    "{primary:#}; original raw-query cleanup returned HTTP {}",
                    response.status
                ),
                Err(cleanup) => {
                    anyhow::bail!("{primary:#}; original raw-query cleanup: {cleanup:#}")
                }
            },
            None => anyhow::bail!(
                "{primary:#}; original raw-query cleanup UNKNOWN: no valid returned continuation"
            ),
        },
    }
}

fn assert_raw_query_size(actor: &TrinoClient, sql: &str, too_large: bool) -> Result<()> {
    let post = actor.submit(sql)?;
    let mut cleanup_uri = post.next_uri().ok().flatten();
    let phase = (|| -> Result<()> {
        let initial = post.assert_success()?;
        let query_id = initial["id"]
            .as_str()
            .filter(|id| !id.is_empty())
            .context("query-size POST omitted original identity")?;
        ensure!(
            initial["stats"]["state"] == "QUEUED",
            "query-size POST was not queued"
        );
        for field in ["updateType", "columns", "data", "updateCount"] {
            ensure!(
                initial.get(field).is_none(),
                "query-size pre-admission POST exposed {field}"
            );
        }
        let mut uri = post
            .next_uri()?
            .context("query-size POST omitted normal registered continuation")?;
        let mut value_seen = false;
        loop {
            let page = actor.get(&uri)?;
            cleanup_uri = Some(uri.clone());
            ensure!(
                page.status == 200,
                "query size used an HTTP transport error instead of a registered result"
            );
            let value = page.json()?;
            ensure!(
                value["id"].as_str() == Some(query_id),
                "query-size identity changed"
            );
            let replay = actor.get(&uri)?;
            ensure!(
                page.status == replay.status
                    && page.body == replay.body
                    && page.semantic_headers() == replay.semantic_headers(),
                "query-size same-token replay changed bytes or repeated headers"
            );
            let next = page.next_uri()?;
            if too_large {
                for field in ["updateType", "columns", "data", "updateCount"] {
                    ensure!(
                        value.get(field).is_none(),
                        "oversized query exposed success field {field}"
                    );
                }
                if let Some(error) = value.get("error") {
                    ensure!(
                        error["errorCode"] == 35
                            && error["errorName"] == "QUERY_TEXT_TOO_LARGE"
                            && error["errorType"] == "USER_ERROR",
                        "query size lost literal35 identity: {error}"
                    );
                    ensure!(next.is_none(), "query-size error retained a continuation");
                    return Ok(());
                }
                ensure!(
                    next.is_some(),
                    "oversized query ended without its required error"
                );
            } else {
                ensure!(
                    value.get("error").is_none(),
                    "exact-limit query failed: {}",
                    value["error"]
                );
                ensure!(
                    value.get("updateType").is_none() && value.get("updateCount").is_none(),
                    "exact-limit SELECT became a completion"
                );
                if let Some(data) = value.get("data") {
                    ensure!(
                        !value_seen && *data == json!([[1]]),
                        "exact-limit input did not return exactly one integer1"
                    );
                    ensure!(
                        next.is_some(),
                        "exact-limit data omitted its acknowledgement URI"
                    );
                    value_seen = true;
                }
                if next.is_none() {
                    ensure!(
                        value["stats"]["state"] == "FINISHED"
                            && value_seen
                            && value.get("data").is_none(),
                        "exact-limit query did not finish after its actual one-row data page"
                    );
                    return Ok(());
                }
            }
            uri = next.context("query-size page omitted continuation")?;
        }
    })();
    finish_raw_registered(actor, cleanup_uri, phase)
}
fn statement_size(context: &mut ScenarioContext, actor: &TrinoClient) -> Result<()> {
    // Independent Trino483 default contract; do not import product constants.
    const UTF16_UNITS: usize = 1_000_000;
    fn padded(bytes: usize) -> String {
        const PREFIX: &str = "SELECT 1 /*";
        const SUFFIX: &str = "*/";
        format!(
            "{PREFIX}{}{SUFFIX}",
            "x".repeat(bytes - PREFIX.len() - SUFFIX.len())
        )
    }
    for (bytes, too_large) in [
        (UTF16_UNITS, false),
        (UTF16_UNITS + 1, true),
        (3 * UTF16_UNITS + 1, true),
    ] {
        let sql = padded(bytes);
        ensure!(
            sql.len() == bytes && sql.encode_utf16().count() == bytes,
            "independent query-size byte/code-unit input changed"
        );
        assert_raw_query_size(actor, &sql, too_large)?;
    }
    context.action("default1000000UTF16 exact SQL accepted; +1unit and3000001-byte legal comment SQL produced registered QueryError35/QUERY_TEXT_TOO_LARGE/USER_ERROR with exact replay");
    Ok(())
}

fn info(context: &mut ScenarioContext, actor: &TrinoClient) -> Result<()> {
    let response = actor.get(&format!("{}/v1/info", actor.base()))?;
    ensure!(response.status == 200, "ServerInfo did not return HTTP 200");
    let info = response.json()?;
    ensure!(
        info["nodeVersion"]["version"]
            .as_str()
            .is_some_and(|version| !version.is_empty())
            && info["coordinator"] == true
            && info["starting"] == false
            && info["environment"]
                .as_str()
                .is_some_and(|environment| !environment.is_empty())
            && info["uptime"].as_str().is_some(),
        "invalid ServerInfo shape: {info}"
    );
    let head = actor.head(&format!("{}/v1/statement", actor.base()))?;
    ensure!(
        head.status == 200 && head.body.is_empty(),
        "HEAD statement endpoint has wrong shape"
    );
    let post = actor.submit("SELECT 1")?;
    let queued = post.assert_success()?;
    let id = queued["id"]
        .as_str()
        .context("queued result omitted query ID")?;
    ensure!(
        id.len() == 27
            && id
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'),
        "invalid client query ID shape: {id}"
    );
    let detail_uri = format!("{}/v1/query/{id}", actor.base());
    ensure!(
        queued["infoUri"] == detail_uri,
        "query detail infoUri changed: {queued}"
    );
    ensure!(
        actor.get(&detail_uri)?.status == 404,
        "unsupported query detail URI did not return 404"
    );
    let uri = post.next_uri()?.context("POST omitted URI for heartbeat")?;
    let components = uri.rsplit('/').take(2).collect::<Vec<_>>();
    ensure!(
        components.len() == 2
            && components[0] == "0"
            && components[1].len() == 32
            && components[1]
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "initial nextUri lacks frozen token 0 / 128-bit slug: {uri}"
    );
    let heartbeat = actor.head(&uri)?;
    ensure!(
        heartbeat.status == 200 && heartbeat.body.is_empty(),
        "query HEAD failed"
    );
    ensure!(
        observation(context, "head-before-first-get")?.active == 0,
        "HEAD started query execution"
    );
    ensure!(
        actor.delete(&uri)?.status == 204,
        "registered query cleanup DELETE failed"
    );
    context
        .action("verified ServerInfo and HEAD, including heartbeat without execution side effects");
    Ok(())
}
#[derive(Clone, Debug)]
pub(super) struct Observation {
    pub(super) active: u64,
    pub(super) complete: u64,
    pub(super) failed: u64,
    pub(super) client_cancel: u64,
    pub(super) abandoned: u64,
}
pub(super) fn observation(context: &mut ScenarioContext, label: &str) -> Result<Observation> {
    let response = context
        .handle()
        .frontend_management_get("/metrics?type=json", Duration::from_secs(1))?;
    ensure!(
        response.status == 200,
        "Trino metric source unavailable: HTTP {}",
        response.status
    );
    let value: Value = serde_json::from_str(&response.body)?;
    let rows = value
        .as_array()
        .context("FE metric response is not an array")?;
    let observation = Observation {
        active: metric(rows, "novarocks_trino_queries_active", &[])?,
        complete: metric(
            rows,
            "novarocks_trino_query_settlements_total",
            &[("outcome", "complete")],
        )?,
        failed: metric(
            rows,
            "novarocks_trino_query_settlements_total",
            &[("outcome", "failed")],
        )?,
        client_cancel: metric(
            rows,
            "novarocks_trino_query_settlements_total",
            &[("outcome", "client_cancel")],
        )?,
        abandoned: metric(
            rows,
            "novarocks_trino_query_settlements_total",
            &[("outcome", "abandoned")],
        )?,
    };
    let directory = context
        .scenario_root()
        .join(format!("trino-metrics-{label}"));
    fs::create_dir_all(&directory)?;
    let sequence = fs::read_dir(&directory)?.count();
    use std::io::Write;
    let mut raw = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(directory.join(format!("snapshot-{sequence:06}.json")))?;
    raw.write_all(response.body.as_bytes())?;
    Ok(observation)
}
fn require_original_statement_held(context: &mut ScenarioContext, label: &str) -> Result<()> {
    let response = context
        .handle()
        .frontend_management_get("/v1/frontend/state", Duration::from_secs(1))?;
    ensure!(
        response.status == 200,
        "original statement observation is unavailable"
    );
    let value: Value = serde_json::from_str(&response.body)?;
    let active = value["workload"]["active"]["statement"]
        .as_u64()
        .context("original active-statement observation is absent")?;
    let positions = value["workload"]["governance"]["result_window_positions"]
        .as_array()
        .context("original result-window positions are absent")?;
    ensure!(
        positions.len() == 4,
        "original result-window dimensions changed"
    );
    let client = positions[0]
        .as_u64()
        .context("Client result-window position is not a count")?;
    // The published ResultWindowClass order is Client, Local, Internal, Closing.
    // This is the original workload's allocation observation, independent of registry phase.
    ensure!(
        active > 0 && client > 0,
        "original statement/window released before protocol settlement: {value}"
    );
    fs::write(
        context
            .scenario_root()
            .join(format!("trino-original-owner-{label}.json")),
        response.body,
    )?;
    Ok(())
}
pub(super) fn await_observation(
    context: &mut ScenarioContext,
    label: &str,
    predicate: impl Fn(&Observation) -> bool,
) -> Result<Observation> {
    loop {
        context.remaining("observe Trino registry settlement")?;
        let snapshot = observation(context, label)?;
        if predicate(&snapshot) {
            return Ok(snapshot);
        }
        thread::sleep(POLL);
    }
}
