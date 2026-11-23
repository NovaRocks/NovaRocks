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

//! Original client IDs, control receipts and SQL history in native 1FE+3BE.

use super::{
    await_idle, failed_query, require_error, require_original_statement_held, success,
    temporal_result,
};
use crate::{
    actors::trino_protocol::{RawResponse, TrinoClient},
    scenario::ScenarioContext,
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    thread,
    time::{Duration, Instant},
};

const CLIENT_TIMEOUT_MS: u64 = 6_000;
const HISTORY_MAX_AGE_MS: u64 = 20_000;
const HISTORY_MAX_BYTES: usize = 1_048_576;
const POLL: Duration = Duration::from_millis(100);
const ADMINISTRATIVE: (i64, &str, &str) = (38, "ADMINISTRATIVELY_KILLED", "USER_ERROR");
const NOT_FOUND: (i64, &str, &str) = (5, "NOT_FOUND", "USER_ERROR");
const NOT_SUPPORTED: (i64, &str, &str) = (13, "NOT_SUPPORTED", "USER_ERROR");

pub(super) fn frontend_overlay() -> String {
    format!(
        "[standalone_server]\ntrino_client_timeout_ms = {CLIENT_TIMEOUT_MS}\n\n\
         [runtime.frontend_workload.query_history]\nmax_bytes = {HISTORY_MAX_BYTES}\nmax_age_ms = {HISTORY_MAX_AGE_MS}\n"
    )
}

struct OriginalQuery {
    id: String,
    uri: String,
    open: bool,
}

impl OriginalQuery {
    fn get(&mut self, actor: &TrinoClient) -> Result<RawResponse> {
        let page = actor.get(&self.uri)?;
        if page.status == 200 {
            if let Some(uri) = page.next_uri()? {
                self.uri = uri;
            } else {
                self.open = false;
            }
        } else if page.status == 404 {
            self.open = false;
        }
        Ok(page)
    }

    fn cancel(&self, actor: &TrinoClient) -> Result<()> {
        let response = actor.delete(&self.uri)?;
        ensure!(
            response.status == 204,
            "DELETE returned HTTP {}",
            response.status
        );
        Ok(())
    }
}

fn with_original_query<T>(
    actor: &TrinoClient,
    sql: &str,
    body: impl FnOnce(&mut OriginalQuery) -> Result<T>,
) -> Result<T> {
    let post = actor.submit(sql)?;
    let initial = post.assert_success()?;
    let mut query = OriginalQuery {
        id: initial["id"]
            .as_str()
            .context("POST omitted original query ID")?
            .to_owned(),
        uri: post.next_uri()?.context("POST omitted original nextUri")?,
        open: true,
    };
    let outcome = (|| {
        ensure!(
            initial["stats"]["state"] == "QUEUED",
            "POST state changed: {initial}"
        );
        let value = body(&mut query)?;
        ensure!(!query.open, "scenario left its original HTTP query open");
        Ok(value)
    })();
    if query.open {
        let cleanup = (|| -> Result<()> {
            let cancelled = actor.delete(&query.uri)?;
            match cancelled.status {
                204 => {
                    let page = query.get(actor)?;
                    ensure!(
                        page.status == 404 || page.status == 200 && page.next_uri()?.is_none(),
                        "cleanup did not observe original URI retirement or terminal response",
                    );
                }
                404 => {}
                other => anyhow::bail!("original cleanup DELETE returned HTTP {other}"),
            }
            Ok(())
        })();
        if let Err(cleanup) = cleanup {
            return match outcome {
                Err(cause) => {
                    Err(cause.context(format!("original HTTP cleanup failed: {cleanup:#}")))
                }
                Ok(_) => Err(cleanup),
            };
        }
    }
    outcome
}

const QUERY_COLUMNS: [&str; 15] = [
    "query_id",
    "state",
    "user",
    "source",
    "query",
    "resource_group_id",
    "queued_time_ms",
    "analysis_time_ms",
    "planning_time_ms",
    "created",
    "started",
    "last_heartbeat",
    "end",
    "error_type",
    "error_code",
];
type QueryRow = BTreeMap<&'static str, Value>;

fn observe(actor: &TrinoClient, id: &str) -> Result<Option<QueryRow>> {
    let select = QUERY_COLUMNS.map(|name| format!(r#""{name}""#)).join(", ");
    let sql = format!("SELECT {select} FROM system.runtime.queries WHERE query_id = '{id}'");
    let (columns, mut rows) = temporal_result(actor, &sql)?;
    let contents = columns
        .as_array()
        .context("query system table omitted column metadata")?;
    ensure!(
        contents.len() == QUERY_COLUMNS.len(),
        "query observation changed width"
    );
    for (column, name) in contents.iter().zip(QUERY_COLUMNS) {
        ensure!(
            column["name"] == name,
            "query observation label changed: {column}"
        );
        if matches!(name, "created" | "started" | "last_heartbeat" | "end") {
            ensure!(
                column["type"] == "timestamp(3) with time zone"
                    && column["typeSignature"]
                        == json!({
                            "rawType": "timestamp with time zone",
                            "arguments": [{"kind": "LONG", "value": 3}],
                        }),
                "unexpected time signature: {column}"
            );
        }
    }
    ensure!(rows.len() < 2, "query ID selected multiple runtime rows");
    let Some(row) = rows.pop() else {
        return Ok(None);
    };
    let values = row.as_array().context("invalid observed query row")?;
    ensure!(
        values.len() == QUERY_COLUMNS.len() && values[0] == id,
        "query table did not return the exact projection and identity: {row}"
    );
    Ok(Some(
        QUERY_COLUMNS
            .into_iter()
            .zip(values.iter().cloned())
            .collect(),
    ))
}

fn present(actor: &TrinoClient, id: &str) -> Result<QueryRow> {
    observe(actor, id)?.with_context(|| format!("query {id} absent from observation"))
}

fn await_state(
    context: &ScenarioContext,
    actor: &TrinoClient,
    id: &str,
    state: &str,
    ready: impl Fn(&QueryRow) -> bool,
) -> Result<QueryRow> {
    loop {
        context.remaining("await original query state")?;
        let row = present(actor, id)?;
        if row["state"] == state && ready(&row) {
            return Ok(row);
        }
        ensure!(
            row["state"] != "FINISHED" && row["state"] != "FAILED",
            "expected {state}, query reached a different terminal state: {row:?}"
        );
        thread::sleep(POLL);
    }
}

fn expect_terminal(row: &QueryRow, state: &str, error: Option<&str>) -> Result<()> {
    ensure!(row["state"] == state && row["end"].is_string(), "{row:?}");
    let kind = error.map(|_| "USER_ERROR");
    ensure!(
        row["error_type"] == json!(kind) && row["error_code"] == json!(error),
        "{row:?}"
    );
    Ok(())
}

fn kill_sql(id: &str, message: Option<&str>) -> String {
    let target = format!("query_id => '{}'", id.replace('\'', "''"));
    let arguments = message.map_or_else(
        || target.clone(),
        |message| format!("{target}, message => '{}'", message.replace('\'', "''")),
    );
    format!("CALL system.runtime.kill_query({arguments})")
}

fn require_admin_result(
    actor: &TrinoClient,
    query: &mut OriginalQuery,
    user: &str,
    message: &str,
) -> Result<()> {
    let page = query.get(actor)?;
    let value = require_error(&page, ADMINISTRATIVE)?;
    let expected = format!("Query killed by user '{user}'. Message: {message}");
    ensure!(
        value["id"] == query.id && value["error"]["message"] == expected,
        "target terminal page did not preserve the management receipt: {value}"
    );
    let replay = actor.get(&query.uri)?;
    ensure!(
        page.status == replay.status
            && page.body == replay.body
            && page.semantic_headers() == replay.semantic_headers(),
        "management failure terminal page changed during replay"
    );
    let row = present(actor, &query.id)?;
    expect_terminal(&row, "FAILED", Some("ADMINISTRATIVELY_KILLED"))
}

fn queued_cancel(context: &mut ScenarioContext, actor: &TrinoClient) -> Result<()> {
    const SQL: &str = "SELECT BIGINT '231' AS queued_probe";
    let queued_actor = actor.with_session(&[("x-trino-source", "queued-observation")]);
    with_original_query(&queued_actor, SQL, |query| {
        let queued = present(actor, &query.id)?;
        ensure!(
            queued["state"] == "QUEUED"
                && queued["user"] == context.sql_user()
                && queued["source"] == "queued-observation"
                && queued["query"] == SQL
                && queued["created"].is_string()
                && queued["last_heartbeat"].is_string()
                && queued["queued_time_ms"].as_u64().is_some(),
            "POST facts were lost before the original GET: {queued:?}"
        );
        for column in [
            "resource_group_id",
            "analysis_time_ms",
            "planning_time_ms",
            "started",
            "end",
            "error_type",
            "error_code",
        ] {
            ensure!(
                queued[column].is_null(),
                "queued observation invented {column}: {queued:?}"
            );
        }
        let message = "queued cancellation receipt";
        success(actor, &kill_sql(&query.id, Some(message)))?;
        require_admin_result(actor, query, context.sql_user(), message)?;
        let ended = present(actor, &query.id)?;
        ensure!(
            ended["started"].is_null()
                && ended["analysis_time_ms"].is_null()
                && ended["planning_time_ms"].is_null()
                && ended["query"] == SQL,
            "pre-GET cancellation started work or lost the original SQL: {ended:?}"
        );
        failed_query(actor, &kill_sql(&query.id, None), NOT_SUPPORTED)?;
        context.action("observed unclaimed SQL, canceled its original ID, and replayed the immutable failure page");
        Ok(())
    })
}

fn expiry(context: &mut ScenarioContext, actor: &TrinoClient) -> Result<String> {
    with_original_query(actor, "SELECT BIGINT '232' AS expired_probe", |query| {
        let abandoned = await_state(context, actor, &query.id, "FAILED", |_| true)?;
        expect_terminal(&abandoned, "FAILED", Some("ABANDONED_QUERY"))?;
        ensure!(
            abandoned["started"].is_null(),
            "unclaimed expiry started work"
        );
        ensure!(
            query.get(actor)?.status == 404,
            "expired original URI did not return HTTP 404"
        );
        failed_query(actor, &kill_sql(&query.id, None), NOT_SUPPORTED)?;
        context.action("observed expired unclaimed URI return HTTP 404 while SQL history retained ABANDONED_QUERY");
        Ok(query.id.clone())
    })
}

fn running_cancel(context: &mut ScenarioContext, actor: &TrinoClient) -> Result<()> {
    with_original_query(actor, "SELECT sleep(60)", |query| {
        let started = query.get(actor)?;
        started.assert_success()?;
        ensure!(
            query.open,
            "running target reached its terminal page before cancellation"
        );
        let running = await_state(context, actor, &query.id, "RUNNING", |row| {
            row["started"].is_string()
        })?;
        ensure!(
            running["analysis_time_ms"].as_u64().is_some()
                && running["planning_time_ms"].as_u64().is_some()
                && running["last_heartbeat"].is_string(),
            "running query lost original compiler or heartbeat facts: {running:?}"
        );
        require_original_statement_held(context, "query-observation-running")?;
        with_original_query(
            actor,
            "SELECT BIGINT '233' AS independent_probe",
            |independent| {
                let message = "running cancellation receipt";
                success(actor, &kill_sql(&query.id, Some(message)))?;
                query.cancel(actor)?;
                require_admin_result(actor, query, context.sql_user(), message)?;
                let mut rows = Vec::new();
                while independent.open {
                    let page = independent.get(actor)?;
                    let body = page.assert_success()?;
                    if let Some(data) = body["data"].as_array() {
                        rows.extend(data.iter().cloned());
                    }
                    if !independent.open {
                        ensure!(
                            body["stats"]["state"] == "FINISHED",
                            "independent query failed"
                        );
                    }
                }
                ensure!(
                    rows == [json!([233])],
                    "wrong independent query rows {rows:?}"
                );
                expect_terminal(&present(actor, &independent.id)?, "FINISHED", None)?;
                failed_query(actor, &kill_sql(&independent.id, None), NOT_SUPPORTED)?;
                Ok(())
            },
        )?;
        context.action("management won a running query's original receipt before DELETE, and an independent query finished");
        Ok(())
    })
}

fn finishing_delivery(context: &mut ScenarioContext, actor: &TrinoClient) -> Result<()> {
    with_original_query(
        actor,
        "INSERT INTO trc1_catalog.first.items VALUES (91)",
        |query| {
            loop {
                let page = query.get(actor)?;
                let body = page.assert_success()?;
                ensure!(
                    query.open,
                    "insertion settled delivery before acknowledging its count row"
                );
                if let Some(data) = body.get("data") {
                    ensure!(
                        *data == json!([[1]]) && body["updateType"] == "INSERT",
                        "{body}"
                    );
                    break;
                }
            }
            let finishing = await_state(context, actor, &query.id, "FINISHING", |_| true)?;
            ensure!(
                finishing["end"].is_null() && finishing["error_code"].is_null(),
                "{finishing:?}"
            );
            failed_query(actor, &kill_sql(&query.id, None), NOT_SUPPORTED)?;
            query.cancel(actor)?;
            require_error(&query.get(actor)?, (3, "USER_CANCELED", "USER_ERROR"))?;
            expect_terminal(&present(actor, &query.id)?, "FAILED", Some("USER_CANCELED"))?;
            let (_, rows) = temporal_result(
                actor,
                "SELECT id FROM trc1_catalog.first.items WHERE id = 91",
            )?;
            ensure!(
                rows == [json!([91])],
                "DELETE overturned the sealed Iceberg insert"
            );
            context.action("rejected management cancellation in FINISHING and preserved a sealed Iceberg insert through failed HTTP delivery");
            Ok(())
        },
    )
}

pub(super) fn run(
    context: &mut ScenarioContext,
    client: &TrinoClient,
    epoch: Instant,
) -> Result<()> {
    let actor = client.with_session(&[
        ("x-trino-client-capabilities", "PARAMETRIC_DATETIME"),
        ("x-trino-time-zone", "UTC"),
    ]);
    queued_cancel(context, &actor)?;
    running_cancel(context, &actor)?;
    await_idle(
        context,
        "trino-query-observation",
        "after-running-kill",
        epoch,
    )?;
    finishing_delivery(context, &actor)?;
    with_original_query(
        &actor,
        "DROP CATALOG IF EXISTS trc7_absent_timing_probe",
        |query| {
            while query.open {
                context.remaining("observe catalog control command")?;
                query.get(&actor)?.assert_success()?;
            }
            let row = present(&actor, &query.id)?;
            expect_terminal(&row, "FINISHED", None)?;
            ensure!(
                row["started"].is_string()
                    && row["analysis_time_ms"].is_null()
                    && row["planning_time_ms"].is_null(),
                "catalog control action lost its own start or invented a SQL phase: {row:?}"
            );
            Ok(())
        },
    )?;
    let expired = expiry(context, &actor)?;

    failed_query(
        &actor,
        &kill_sql("20000101_000000_00000_aaaaa", None),
        NOT_FOUND,
    )?;
    failed_query(
        &actor,
        "CALL system.runtime.kill_query(query_id => 'invalid query id')",
        (30, "INVALID_PROCEDURE_ARGUMENT", "USER_ERROR"),
    )?;
    loop {
        context.remaining("observe bounded history expiry")?;
        if observe(&actor, &expired)?.is_none() {
            break;
        }
        thread::sleep(POLL);
    }
    failed_query(&actor, &kill_sql(&expired, None), NOT_FOUND)?;
    context.action("distinguished invalid, never registered, retained terminal and evicted IDs");
    Ok(())
}
