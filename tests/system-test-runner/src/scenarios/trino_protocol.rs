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
    actors::{
        mysql as mysql_actor,
        trino_protocol::{RawResponse, TrinoClient},
    },
    scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig},
};
use anyhow::{Context, Result, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::{
    CrossProcessConfigOverlay, ServerHandle,
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
        Case::MysqlPositions,
        Case::Drain,
        Case::Refusals,
        Case::RowBudget,
        Case::UpdateType,
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
    MysqlPositions,
    Drain,
    Refusals,
    RowBudget,
    UpdateType,
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
            Case::MysqlPositions => "trino-protocol/independent-of-mysql-positions",
            Case::Drain => "trino-protocol/drain",
            Case::Refusals => "trino-protocol/refusals",
            Case::RowBudget => "trino-protocol/row-exceeds-page-budget",
            Case::UpdateType => "trino-protocol/ddl-update-type",
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
            context.mysql_user(),
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
            Case::MysqlPositions => mysql_positions(context, &actor),
            Case::Drain => drain(context, &actor),
            Case::Refusals => refusals(context, &actor),
            Case::RowBudget => row_budget(context, &actor),
            Case::UpdateType => {
                self.prepare_catalog(context, &actor)?;
                update_types(context, &actor)
            }
            Case::Info => info(context, &actor),
        };
        if matches!(self.case, Case::Session | Case::UpdateType) {
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
fn require_error(response: &RawResponse, expected: &str) -> Result<Value> {
    ensure!(
        response.status == 200,
        "expected a Trino QueryError, got HTTP {}",
        response.status
    );
    let value = response.json()?;
    ensure!(
        value["error"]["errorName"] == expected,
        "expected {expected}, got {value}"
    );
    ensure!(
        value.get("nextUri").is_none(),
        "error page contains nextUri"
    );
    Ok(value)
}
fn failed_query(actor: &TrinoClient, sql: &str, expected: &str) -> Result<Value> {
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
            replay.body == page.body,
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
                    replay.body == page.body,
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
            let partial = actor.interrupt(&uri, 128)?;
            ensure!(
                page.body.starts_with(&partial.body) && partial.body.len() < page.body.len(),
                "interrupted response did not preserve a strict body prefix"
            );
            let replay = actor.get(&uri)?;
            ensure!(
                replay.body == page.body && replay.semantic_headers() == page.semantic_headers(),
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
    context.action("dropped a partial HTTP response, replayed identical full bytes, and completed the same query");
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
    failed_query(&timed, "SELECT sleep(3)", "EXCEEDED_TIME_LIMIT")?;
    failed_query(
        &actor.with_session(&[("x-trino-session", "trc_unknown_property=1")]),
        "SELECT 1",
        "INVALID_SESSION_PROPERTY",
    )?;
    failed_query(
        &actor.with_session(&[("x-trino-time-zone", "Mars/Olympus")]),
        "SELECT 1",
        "NOT_SUPPORTED",
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
    context.action("verified name headers, USE effects after dropped current schema, SET round-trip behavior, and typed session refusals");
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
    require_error(&actor.get(&uri)?, "USER_CANCELED")?;
    ensure!(
        actor.delete(&uri)?.status == 204,
        "repeated DELETE is not idempotent"
    );
    require_error(&actor.get(&uri)?, "USER_CANCELED")?;
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
        require_error(&result, "ABANDONED_QUERY")?;
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
fn mysql_positions(context: &mut ScenarioContext, actor: &TrinoClient) -> Result<()> {
    let epoch = Instant::now();
    let first = success(actor, "SELECT 1")?;
    let terminal_uri = first
        .iter()
        .rev()
        .skip(1)
        .find_map(|page| page.next_uri().transpose())
        .transpose()?
        .context("first query omitted terminal ACK URI")?;
    let first_terminal = terminal(&first)?.body.clone();
    for _ in 1..513 {
        success(actor, "SELECT 1")?;
    }
    ensure!(
        epoch.elapsed() < Duration::from_secs(300),
        "513 short queries did not overlap within the configured abandonment window"
    );
    ensure!(
        actor.get(&terminal_uri)?.body == first_terminal,
        "first tombstone did not survive the >512-query window"
    );
    let connection = mysql_actor::connect_until(
        context.mysql_user(),
        context.mysql_port(),
        context.deadline(),
    )?;
    connection.run(|connection| {
        let result: Vec<u64> = connection.query("SELECT 1")?;
        ensure!(
            result == vec![1],
            "MySQL entrypoint failed after Trino tombstones"
        );
        Ok(())
    })?;
    context.action("completed 513 Trino statements within one tombstone window and verified a fresh MySQL query");
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
    require_error(&refusal, "SERVER_SHUTTING_DOWN")?;
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
            "NOT_SUPPORTED",
        )?;
    }
    let error = failed_query(
        actor,
        "SELECT CAST(1 AS LARGEINT) AS wide_value",
        "NOT_SUPPORTED",
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
        "GENERIC_INSUFFICIENT_RESOURCES",
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
        let pages = success(actor, sql)?;
        let end = terminal(&pages)?.json()?;
        ensure!(
            end["updateType"] == expected,
            "wrong updateType for {sql}: {end}"
        );
        ensure!(
            end.get("columns")
                .is_none_or(|columns| columns.as_array().is_some_and(Vec::is_empty)),
            "completion-only statement exposed columns"
        );
    }
    context.action("verified DDL and SET updateType names with no row columns");
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
struct Observation {
    active: u64,
    complete: u64,
    failed: u64,
    client_cancel: u64,
    abandoned: u64,
}
fn observation(context: &mut ScenarioContext, label: &str) -> Result<Observation> {
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
fn await_observation(
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
