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

use super::result_delivery_progress::finish_original_body;
use super::result_delivery_support::await_idle;
use super::trino_protocol::{await_observation, observation, require_error};
use crate::actors::sql::{self, SqlSession};
use crate::actors::trino_protocol::{RawResponse, TrinoClient};
use crate::scenario::{Scenario, ScenarioContext};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::TestClusterOwner;
use serde_json::json;
use std::time::{Duration, Instant};

const CELL_BYTES: usize = 1_048_576;
const CANCELED: (i64, &str, &str) = (3, "USER_CANCELED", "USER_ERROR");
const OVERSIZED: (i64, &str, &str) = (
    131072,
    "GENERIC_INSUFFICIENT_RESOURCES",
    "INSUFFICIENT_RESOURCES",
);

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![Box::new(Closing::Unread), Box::new(Closing::Interrupted)]
}

enum Closing {
    Unread,
    Interrupted,
}

impl Scenario for Closing {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        super::declarations::for_module(
            "result_delivery_closing",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        match self {
            Self::Unread => "result-delivery/unread-body-cancel",
            Self::Interrupted => "result-delivery/interrupted-large-body-replay",
        }
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(context.handle().be_count() == 3, "closing requires 1FE+3BE");
        let epoch = Instant::now();
        await_idle(context, "trino-closing", "before", epoch)?;
        let deadline = Instant::now()
            + context
                .remaining("Trino closing")?
                .min(Duration::from_secs(20));
        let actor = TrinoClient::new(
            context.trino_port(),
            context.sql_user(),
            deadline,
            &context.scenario_root().join("closing-http"),
        )?;
        let mut live = None;
        let mut held = None;
        let mut joins = Vec::new();
        let mut phases = Vec::new();
        let primary = (|| -> Result<()> {
            if matches!(self, Self::Interrupted) {
                let before = observation(context, "oversized-before")?;
                let (response, _) = advance(&actor, &large_sql(17), &mut live)?;
                require_error(&response, OVERSIZED)?;
                let after =
                    await_observation(context, "oversized-after", |state| state.active == 0)?;
                ensure!(
                    after.failed == before.failed + 1
                        && after.complete == before.complete
                        && after.client_cancel == before.client_cancel
                        && after.abandoned == before.abandoned,
                    "original seventeen-column row did not settle one resource failure"
                );
            }
            let before = observation(context, "closing-before")?;
            let (sql, names, byte) = match self {
                Self::Unread => (
                    "SELECT REPEAT('x', 1048576) AS payload FROM generate_series(1, 1)".to_owned(),
                    vec!["payload"],
                    b'x',
                ),
                Self::Interrupted => (large_sql(3), vec!["c0", "c1", "c2"], b'q'),
            };
            let started = Instant::now();
            let (page, token) = advance(&actor, &sql, &mut live)?;
            ensure!(
                started.elapsed() < Duration::from_secs(5),
                "first data page exceeded its observation budget"
            );
            let token = token.context("query ended without its data page")?;
            let next = live.clone().context("data page has no continuation")?;
            assert_row(&page, &names, byte)?;
            let id = page.json()?["id"]
                .as_str()
                .context("data page has no id")?
                .to_owned();
            let held_start = Instant::now();
            let reader = held.insert(actor.open_get(&token)?);
            let at_headers = reader.observe_held()?;
            ensure!(
                at_headers.read_calls == 0
                    && at_headers.read_bytes == 0
                    && !at_headers.first_byte_observed
                    && !at_headers.eof_observed,
                "body advanced at headers"
            );

            phases.push(json!({"phase":"held-headers","elapsed_us":epoch.elapsed().as_micros(),"body":at_headers}));
            let terminal = match self {
                Self::Unread => {
                    let delete_started = Instant::now();
                    ensure!(
                        held_start.elapsed() < Duration::from_secs(5),
                        "held headers exceeded their observation budget"
                    );
                    ensure!(actor.delete(&next)?.status == 204, "DELETE failed");
                    let state = reader.observe_held()?;
                    ensure!(
                        state.read_calls == 0 && state.read_bytes == 0 && !state.eof_observed,
                        "original reader advanced during DELETE"
                    );
                    phases.push(json!({"phase":"delete-returned","elapsed_us":epoch.elapsed().as_micros(),"body":state}));
                    let original = finish_original_body(&mut held, Vec::new(), &mut joins)?;
                    assert_replay(&original, &page)?;
                    let terminal = actor.get(&next)?;
                    require_error(&terminal, CANCELED)?;
                    ensure!(
                        delete_started.elapsed() < Duration::from_secs(2),
                        "closing observation exceeded its control budget"
                    );
                    terminal
                }
                Self::Interrupted => {
                    let prefix = reader.read_prefix(128)?;
                    let partial = RawResponse {
                        status: reader.status(),
                        headers: reader.headers().clone(),
                        body: prefix,
                    };
                    ensure!(
                        held_start.elapsed() < Duration::from_secs(5),
                        "held prefix exceeded its observation budget"
                    );
                    let interrupted_at = Instant::now();
                    let interrupted = held
                        .take()
                        .context("original reader disappeared")?
                        .interrupt();
                    let clean = interrupted.require_clean();
                    joins.push(interrupted);
                    clean?;
                    let state = &joins
                        .last()
                        .context("missing original reader join")?
                        .observation;
                    ensure!(
                        state.first_byte_observed
                            && state.read_bytes == 128
                            && !state.eof_observed
                            && partial.body.len() == 128
                            && partial.body.len() < page.body.len()
                            && page.body.starts_with(&partial.body)
                            && partial.status == page.status
                            && partial.semantic_headers() == page.semantic_headers(),
                        "reader did not stop at the original strict prefix"
                    );
                    let replay = actor.get(&token)?;
                    assert_replay(&replay, &page)?;
                    ensure!(
                        interrupted_at.elapsed() < Duration::from_secs(2),
                        "interruption replay exceeded its control budget"
                    );
                    let terminal = actor.get(&next)?;
                    let value = terminal.assert_success()?;
                    ensure!(
                        terminal.next_uri()?.is_none()
                            && value["stats"]["state"] == "FINISHED"
                            && value.get("data").is_none(),
                        "large row did not reach its zero-row FINISHED page"
                    );
                    terminal
                }
            };
            phases.push(json!({"phase":"terminal-after-body-join","elapsed_us":epoch.elapsed().as_micros()}));
            ensure!(
                terminal.json()?["id"] == id,
                "terminal query identity changed"
            );
            ensure!(actor.delete(&next)?.status == 204, "terminal DELETE failed");
            assert_replay(&actor.get(&next)?, &terminal)?;
            live = None;
            let after = await_observation(context, "closing-after", |state| state.active == 0)?;
            ensure!(
                after.complete == before.complete + u64::from(matches!(self, Self::Interrupted))
                    && after.client_cancel
                        == before.client_cancel + u64::from(matches!(self, Self::Unread))
                    && after.failed == before.failed
                    && after.abandoned == before.abandoned,
                "closing changed settlement counters unexpectedly"
            );
            Ok(())
        })();
        let cleanup = live
            .as_deref()
            .map(|uri| {
                actor.delete(uri).and_then(|response| {
                    ensure!(
                        response.status == 204,
                        "cleanup DELETE returned {}",
                        response.status
                    );
                    Ok(())
                })
            })
            .transpose();
        if let Some(original) = held {
            joins.push(original.interrupt());
        }
        let idle = await_idle(context, "trino-closing", "after", epoch);
        let join_checks = joins
            .iter()
            .map(|join| join.require_clean())
            .collect::<Result<Vec<_>>>();
        std::fs::write(
            context.scenario_root().join("closing-reader-joins.json"),
            serde_json::to_vec_pretty(&json!(
                joins
                    .iter()
                    .map(|join| &join.observation)
                    .collect::<Vec<_>>()
            ))?,
        )?;
        std::fs::write(
            context.scenario_root().join("closing-phases.json"),
            serde_json::to_vec_pretty(&phases)?,
        )?;
        if primary.is_err() || cleanup.is_err() || idle.is_err() || join_checks.is_err() {
            anyhow::bail!(
                "closing failed: primary={primary:?}, cleanup={cleanup:?}, idle={idle:?}, readers={join_checks:?}"
            );
        }
        let mut health = SqlSession::new(context.sql_user(), context.trino_port(), deadline)?;
        let result =
            health.execute("SELECT SUM(generate_series) AS total FROM generate_series(1, 100)")?;
        ensure!(
            result.columns.len() == 1
                && result.columns[0].name == "total"
                && result.columns[0].type_signature.raw_type == "bigint"
                && result.columns[0].type_signature.arguments.is_empty()
                && sql::single_integer(&result)? == 5050,
            "post-closing health result changed"
        );
        Ok(())
    }
}

fn large_sql(columns: usize) -> String {
    let fields = (0..columns)
        .map(|index| format!("REPEAT('q', 1048575 + generate_series) AS c{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT {fields} FROM generate_series(1, 1)")
}

fn advance(
    actor: &TrinoClient,
    sql: &str,
    live: &mut Option<String>,
) -> Result<(RawResponse, Option<String>)> {
    let post = actor.submit(sql)?;
    *live = post.next_uri()?;
    let id = post.json()?["id"]
        .as_str()
        .context("POST has no id")?
        .to_owned();
    if live.is_none() {
        return Ok((post, None));
    }
    while let Some(token) = live.clone() {
        let page = actor.get(&token)?;
        let value = page.json()?;
        ensure!(value["id"] == id, "query id changed while fetching data");
        *live = page.next_uri()?;
        if value.get("data").is_some() {
            return Ok((page, Some(token)));
        }
        if live.is_none() {
            return Ok((page, None));
        }
        page.assert_success()?;
    }
    unreachable!("live continuation checked before loop")
}

fn assert_row(page: &RawResponse, names: &[&str], byte: u8) -> Result<()> {
    let value = page.assert_success()?;
    let columns = value["columns"].as_array().context("missing columns")?;
    let rows = value["data"].as_array().context("missing row")?;
    ensure!(
        columns.len() == names.len() && rows.len() == 1,
        "row shape changed"
    );
    let row = rows[0].as_array().context("row is not an array")?;
    ensure!(row.len() == names.len(), "row width changed");
    for ((column, cell), name) in columns.iter().zip(row).zip(names) {
        ensure!(
            column["name"] == *name
                && column["typeSignature"]["rawType"] == "varchar"
                && column["typeSignature"]["arguments"]
                    == json!([{"kind":"LONG","value":2147483647}]),
            "unexpected column declaration for {name}"
        );
        let cell = cell.as_str().context("cell is not a string")?;
        ensure!(
            cell.len() == CELL_BYTES && cell.bytes().all(|actual| actual == byte),
            "literal contents changed for {name}"
        );
    }
    Ok(())
}

fn assert_replay(actual: &RawResponse, original: &RawResponse) -> Result<()> {
    ensure!(
        actual.status == original.status
            && actual.semantic_headers() == original.semantic_headers()
            && actual.body == original.body,
        "replay changed HTTP status, repeated headers, or body bytes"
    );
    Ok(())
}
