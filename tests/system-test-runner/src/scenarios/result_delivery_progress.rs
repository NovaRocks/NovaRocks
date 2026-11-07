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

//! Independent requests progress while the original Trino consumer pauses
//! continuation or holds its actual HTTP body and Native Data window is full.

use super::result_delivery::root_census;
use super::result_delivery_support::await_idle;
use crate::actors::sql::{self, SqlSession};
use crate::actors::trino_protocol::{
    HeldHttpResponse, JoinedHttpResponse, RawResponse, TrinoClient,
};
use crate::scenario::{Scenario, ScenarioContext};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::TestClusterOwner;
use novarocks_result_contract::RootProfileV1;
use novarocks_test_trino_client::{Column, TrinoError, TypeArgument};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

const ROWS: u64 = 8192;
const BASE_CELL_BYTES: usize = 1024;
const READ_BYTES: usize = 65536;
// Keep the former raw actor's body bound, now per page, never per result.
const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy)]
enum Mode {
    Slow,
    Unread,
}
struct ClientProgress(Mode);
pub(super) fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(ClientProgress(Mode::Slow)),
        Box::new(ClientProgress(Mode::Unread)),
    ]
}
impl Scenario for ClientProgress {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "result_delivery_progress",
            self.name(),
            true,
            false,
        )
    }
    fn name(&self) -> &'static str {
        match self.0 {
            Mode::Slow => "result-delivery/slow-client-progress",
            Mode::Unread => "result-delivery/unread-client-progress",
        }
    }
    fn is_explicit_stage(&self) -> bool {
        true
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "client progress requires native 1FE+3BE"
        );
        let epoch = Instant::now();
        await_idle(context, "client-progress", "before", epoch)?;
        // Preserve the original actor clock. No later request renews it.
        let deadline = Instant::now()
            + context
                .remaining("held client progress")?
                .min(Duration::from_secs(90));
        let actor = TrinoClient::new(
            context.trino_port(),
            context.sql_user(),
            deadline,
            &context.scenario_root().join("client-progress-http"),
        )?;
        let mut live_uri = None;
        let mut oracle = PrefixOracle::default();
        let mut held: Option<HeldHttpResponse> = None;
        let mut joined = Vec::new();
        let mut census = Vec::new();
        let mut independent = Vec::new();
        let mut prefix = Vec::new();
        let mut held_samples = Vec::new();
        let mut delete_started_us = None;
        let mut delete_returned_us = None;
        let mut body_joined_us = None;
        let mut terminal_error_us = None;
        let mut first_page_rows = 0;
        let mut failures = Vec::new();
        let primary = (|| -> Result<()> {
            let query = format!(
                "SELECT REPEAT('q', {BASE_CELL_BYTES} + generate_series) AS payload FROM generate_series(1, {ROWS}) ORDER BY generate_series"
            );
            let post = actor.submit(&query)?;
            oracle.id = post.json()?["id"]
                .as_str()
                .context("POST omitted original query identity")?
                .to_owned();
            live_uri = post.next_uri()?;
            live_uri = oracle.observe(&post, false)?;
            drop(post);
            while oracle.rows == 0 {
                let page = actor.get(
                    live_uri
                        .as_deref()
                        .context("query ended before its first data page")?,
                )?;
                live_uri = oracle.observe(&page, false)?;
            }
            first_page_rows = oracle.rows;
            ensure!(
                first_page_rows < ROWS && live_uri.is_some(),
                "first data page consumed the complete declared result"
            );
            // No active HTTP reader exists in this phase. The continuation and
            // actual Native producer/window are its separate liveness facts.
            await_saturated_root(
                context,
                deadline,
                epoch,
                "next-get-paused",
                &mut census,
                || Ok(()),
            )?;
            independent_progress(context, deadline, "next-get-paused", &mut independent)?;
            await_saturated_root(
                context,
                deadline,
                epoch,
                "next-get-paused-after-independent",
                &mut census,
                || Ok(()),
            )?;

            held = Some(
                actor.open_get(
                    live_uri
                        .as_deref()
                        .context("held page lost its exact URI")?,
                )?,
            );
            let at_headers = held.as_mut().unwrap().observe_held()?;
            ensure!(
                at_headers.read_calls == 0
                    && at_headers.read_bytes == 0
                    && !at_headers.first_byte_observed
                    && !at_headers.eof_observed,
                "received headers were mistaken for body consumption"
            );
            held_samples.push(json!({"phase":"headers","observation":at_headers}));
            if matches!(self.0, Mode::Slow) {
                let step = held.as_mut().unwrap().read_step(READ_BYTES)?;
                ensure!(
                    !step.eof && !step.bytes.is_empty() && step.bytes.len() <= READ_BYTES,
                    "slow reader did not make its one bounded positive actual body read"
                );
                prefix = step.bytes;
            }
            let controlled_bytes = prefix.len() as u64;
            let controlled_calls = u64::from(matches!(self.0, Mode::Slow));
            let mut assert_held = || -> Result<()> {
                let state = held
                    .as_mut()
                    .context("original body reader was released early")?
                    .observe_held()?;
                ensure!(
                    !state.eof_observed
                        && state.read_calls == controlled_calls
                        && state.read_bytes == controlled_bytes
                        && state.first_byte_observed == (controlled_bytes > 0),
                    "original held body advanced without an explicit read command"
                );
                held_samples.push(json!({"phase":"body-held","elapsed_micros":epoch.elapsed().as_micros(),"observation":state}));
                Ok(())
            };
            await_saturated_root(
                context,
                deadline,
                epoch,
                "body-held",
                &mut census,
                &mut assert_held,
            )?;
            independent_progress(context, deadline, "body-held", &mut independent)?;
            await_saturated_root(
                context,
                deadline,
                epoch,
                "body-held-after-independent",
                &mut census,
                &mut assert_held,
            )?;
            assert_held()?;

            delete_started_us = Some(epoch.elapsed().as_micros());
            ensure!(
                actor
                    .delete(
                        live_uri
                            .as_deref()
                            .context("DELETE lost its original query URI")?
                    )?
                    .status
                    == 204,
                "same-query DELETE did not return its actual accepted status"
            );
            delete_returned_us = Some(epoch.elapsed().as_micros());
            let controlled_sha256 = format!("{:x}", Sha256::digest(&prefix));
            let raw = finish_original_body(&mut held, std::mem::take(&mut prefix), &mut joined)?;
            body_joined_us = Some(epoch.elapsed().as_micros());
            std::fs::write(
                context.scenario_root().join("resumed-held-page.body"),
                &raw.body,
            )?;
            let rows_before = oracle.rows;
            live_uri = oracle.observe(&raw, true)?;
            ensure!(
                oracle.rows > rows_before,
                "held GET did not retain its actual data page"
            );
            drop(raw);
            while oracle.public_error.is_none() {
                let raw = actor.get(
                    live_uri
                        .as_deref()
                        .context("cancelled query omitted its terminal continuation")?,
                )?;
                live_uri = oracle.observe(&raw, true)?;
            }
            terminal_error_us = Some(epoch.elapsed().as_micros());
            ensure!(
                oracle.rows > 0 && oracle.rows < ROWS && live_uri.is_none(),
                "cancelled query lost its verified prefix or completed every declared row"
            );
            held_samples.push(
                json!({"phase":"resumed-and-joined","controlled_body_bytes":controlled_bytes,
                "controlled_prefix_sha256":controlled_sha256,"joined":joined.last()}),
            );
            Ok(())
        })();
        if let Err(error) = primary {
            failures.push(error);
        }
        // Preserve the first actual failure. Cancel this same still-live query
        // before releasing its original reader; never cancel an independent one.
        let mut cleanup_cancelled = delete_returned_us.is_some();
        if delete_returned_us.is_none()
            && let Some(uri) = live_uri.as_deref()
        {
            let cleanup = actor.delete(uri).and_then(|reply| {
                ensure!(reply.status == 204, "cleanup DELETE was not accepted");
                Ok(())
            });
            cleanup_cancelled = cleanup.is_ok();
            if let Err(error) = cleanup {
                failures.push(error.context("cancel original progress query during cleanup"));
            }
        }
        if let Some(original) = held.take() {
            let exit = original.interrupt();
            if let Err(error) = exit.require_clean() {
                failures.push(error);
            }
            joined.push(exit);
        }
        let mut cleanup_terminal = Value::Null;
        if !failures.is_empty()
            && cleanup_cancelled
            && let Some(uri) = live_uri.as_deref()
        {
            match close_cancelled_query(&actor, uri, &oracle.id) {
                Ok(value) => cleanup_terminal = value,
                Err(error) => {
                    failures.push(error.context("observe original cancelled query during cleanup"))
                }
            }
        }
        if let Err(error) = await_idle(context, "client-progress", "after-cancel", epoch) {
            failures.push(error);
        }
        let health = (|| -> Result<Value> {
            let health_deadline = Instant::now()
                + context
                    .remaining("post-cancel health")?
                    .min(Duration::from_secs(15));
            let mut health =
                SqlSession::new(context.sql_user(), context.trino_port(), health_deadline)?;
            let result = health.execute("SELECT 1 AS value")?;
            ensure!(
                sql::single_integer(&result)? == 1,
                "post-cancel health returned another value"
            );
            Ok(
                json!({"query_id":result.query_id,"pages":result.pages,"columns":result.columns,"value":sql::single_integer(&result)?}),
            )
        })();
        let health = match health {
            Ok(value) => value,
            Err(error) => {
                failures.push(error);
                Value::Null
            }
        };
        if let Err(error) = await_idle(context, "client-progress", "after-health", epoch) {
            failures.push(error);
        }
        let receipt = json!({"schema_version":2,"mode":if matches!(self.0,Mode::Slow){"slow"}else{"unread"},
            "execution_budget_seconds":90,"state_observation_budget_seconds":30,
            "declared_rows":ROWS,"base_cell_bytes":BASE_CELL_BYTES,"row_formula":"q repeated (1024 + row), row=1..8192 ascending",
            "query_id":oracle.id,"first_page_rows":first_page_rows,"verified_prefix_rows":oracle.rows,
            "verified_prefix_value_bytes":oracle.value_bytes,"verified_prefix_sha256":format!("{:x}",oracle.hash.clone().finalize()),
            "columns_observed":oracle.columns_seen,"pages":oracle.pages,"public_terminal_error":oracle.public_error,
            "delete_started_us":delete_started_us,"delete_returned_us":delete_returned_us,
            "original_body_joined_us":body_joined_us,"public_terminal_observed_us":terminal_error_us,
            "held_observations":held_samples,"actual_reader_joins":joined,"root_census":census,"independent_queries":independent,
            "post_cancel_health":health,"cleanup_terminal":cleanup_terminal,"actual_failure_sources":failures.len(),
            "scope":"Original HTTP body and actual Native producer/window are separately observed. No TCP receive-buffer, allocator or server socket-write pressure claim. HTTP JSON prefix is not a row. MySQL packet/metadata ordering and physical socket error exemptions retire."});
        let stored = serde_json::to_vec_pretty(&receipt)
            .map_err(anyhow::Error::from)
            .and_then(|bytes| {
                std::fs::write(context.scenario_root().join("client-progress.json"), bytes)
                    .map_err(anyhow::Error::from)
            });
        if let Err(error) = stored {
            failures.push(error);
        }
        if !failures.is_empty() {
            return Err(anyhow::Error::new(ProgressFailures(failures)));
        }
        context.action("verified separate continuation pause and actual held body, independent typed queries, exact DELETE, cancelled row prefix and original reader/owner exits");
        Ok(())
    }
}

#[derive(Default)]
struct PrefixOracle {
    id: String,
    rows: u64,
    value_bytes: u64,
    hash: Sha256,
    columns_seen: bool,
    pages: Vec<Value>,
    public_error: Option<TrinoError>,
}
impl PrefixOracle {
    fn observe(&mut self, raw: &RawResponse, cancelled: bool) -> Result<Option<String>> {
        ensure!(
            raw.status == 200 && raw.body.len() <= MAX_BODY_BYTES,
            "original page status/body exceeds the raw actor contract"
        );
        let value = raw.json()?;
        ensure!(
            value["id"] == self.id,
            "original query page changed identity"
        );
        ensure!(
            value.get("updateType").is_none() && value.get("updateCount").is_none(),
            "ordinary rows became command completion"
        );
        let next = match value.get("nextUri") {
            None | Some(Value::Null) => None,
            Some(Value::String(uri)) => Some(uri.clone()),
            _ => anyhow::bail!("original continuation is not a URI string"),
        };
        let before = self.rows;
        if value.get("error").is_some() {
            // Deserialize the actual raw error object, preserving unknown
            // fields and numeric source spelling rather than re-encoding Value.
            let fields: BTreeMap<String, Box<serde_json::value::RawValue>> =
                serde_json::from_slice(&raw.body)?;
            self.public_error = Some(serde_json::from_str(
                fields
                    .get("error")
                    .context("original error disappeared")?
                    .get(),
            )?);
            let error = self.public_error.as_ref().unwrap();
            ensure!(
                cancelled
                    && error.error_code == 3
                    && error.error_name == "USER_CANCELED"
                    && error.error_type == "USER_ERROR"
                    && next.is_none()
                    && value["stats"]["state"] == "FAILED",
                "original query did not return its typed cancellation"
            );
            ensure!(
                value.get("columns").is_none() && value.get("data").is_none(),
                "cancelled terminal published success rows"
            );
        } else {
            if let Some(columns) = value.get("columns") {
                let columns: Vec<Column> = serde_json::from_value(columns.clone())?;
                ensure!(
                    columns.len() == 1
                        && columns[0].name == "payload"
                        && columns[0].type_name == "varchar"
                        && columns[0].type_signature.raw_type == "varchar"
                        && columns[0].type_signature.arguments
                            == [TypeArgument::Long(2_147_483_647)],
                    "declared payload changed its exact VARCHAR layout"
                );
                self.columns_seen = true;
            }
            if let Some(rows) = value.get("data") {
                ensure!(
                    self.columns_seen,
                    "rows preceded their exact public column declaration"
                );
                for row in rows.as_array().context("page data is not complete rows")? {
                    let row = row.as_array().context("public row is not a JSON array")?;
                    ensure!(
                        row.len() == 1 && self.rows < ROWS,
                        "public prefix has extra cells or rows"
                    );
                    let cell = row[0]
                        .as_str()
                        .context("payload is not a non-null VARCHAR")?;
                    let ordinal = self.rows + 1;
                    ensure!(
                        cell.len() == BASE_CELL_BYTES + usize::try_from(ordinal)?
                            && cell.bytes().all(|byte| byte == b'q'),
                        "public row differs from its independent ascending literal formula"
                    );
                    self.hash.update((cell.len() as u64).to_le_bytes());
                    self.hash.update(cell.as_bytes());
                    self.value_bytes += cell.len() as u64;
                    self.rows = ordinal;
                }
            }
            ensure!(
                next.is_some(),
                "query completed instead of retaining its original cancelable continuation"
            );
        }
        self.pages.push(json!({"body_bytes":raw.body.len(),"body_sha256":format!("{:x}",Sha256::digest(&raw.body)),
            "state":value["stats"]["state"],"rows":self.rows-before,"prefix_rows":self.rows,"has_next_uri":next.is_some(),"cancelled_error":self.public_error.is_some()}));
        Ok(next)
    }
}

pub(super) fn finish_original_body(
    held: &mut Option<HeldHttpResponse>,
    prefix: Vec<u8>,
    joins: &mut Vec<JoinedHttpResponse>,
) -> Result<RawResponse> {
    let original = held.as_mut().context("original body reader disappeared")?;
    let status = original.status();
    let headers = original.headers().clone();
    let mut body = prefix;
    loop {
        let read = original.read_step(READ_BYTES)?;
        ensure!(
            body.len()
                .checked_add(read.bytes.len())
                .is_some_and(|size| size <= MAX_BODY_BYTES),
            "held page exceeds original raw body bound"
        );
        body.extend_from_slice(&read.bytes);
        if read.eof {
            break;
        }
    }
    let joined = held.take().unwrap().settle();
    let checked = joined.require_clean();
    joins.push(joined);
    checked?;
    Ok(RawResponse {
        status,
        headers,
        body,
    })
}
fn independent_progress(
    context: &mut ScenarioContext,
    deadline: Instant,
    phase: &str,
    facts: &mut Vec<Value>,
) -> Result<()> {
    let before = (0..3)
        .map(|backend| {
            context
                .handle()
                .backend_task_execution_tasks_created(backend)
        })
        .collect::<Result<Vec<_>>>()?;
    for (query, expected, expected_type) in [
        ("SELECT 1 AS value", 1, "integer"),
        (
            "SELECT SUM(generate_series) AS value FROM generate_series(1, 100)",
            5050,
            "bigint",
        ),
    ] {
        let mut session = SqlSession::new(context.sql_user(), context.trino_port(), deadline)?;
        let result = session.execute(query)?;
        ensure!(
            result.columns.len() == 1
                && result.columns[0].name == "value"
                && result.columns[0].type_signature.raw_type == expected_type
                && result.columns[0].type_signature.arguments.is_empty()
                && sql::single_integer(&result)? == expected,
            "independent query changed its declared typed integer result"
        );
        facts.push(json!({"phase":phase,"sql":query,"expected":expected,
            "actual":sql::single_integer(&result)?,"query_id":result.query_id,"pages":result.pages,"columns":result.columns}));
    }
    let after = (0..3)
        .map(|backend| {
            context
                .handle()
                .backend_task_execution_tasks_created(backend)
        })
        .collect::<Result<Vec<_>>>()?;
    ensure!(
        before
            .iter()
            .chain(&after)
            .all(|count| count.is_finite() && *count >= 0.0 && count.fract() == 0.0),
        "actual Native task counter is unavailable or invalid"
    );
    ensure!(
        after.iter().sum::<f64>() > before.iter().sum::<f64>(),
        "independent queries did not create actual Native work"
    );
    facts.push(json!({"phase":phase,"before_tasks_created":before,"after_tasks_created":after}));
    Ok(())
}

fn close_cancelled_query(actor: &TrinoClient, uri: &str, id: &str) -> Result<Value> {
    let mut uri = uri.to_owned();
    let mut pages = 0u64;
    loop {
        let raw = actor.get(&uri)?;
        ensure!(
            raw.status == 200 && raw.body.len() <= MAX_BODY_BYTES,
            "original cleanup query returned an invalid status/body"
        );
        let value = raw.json()?;
        ensure!(
            value["id"] == id,
            "cleanup response belongs to another query"
        );
        pages += 1;
        match raw.next_uri()? {
            Some(next) => uri = next,
            None => {
                return Ok(
                    json!({"pages":pages,"state":value["stats"]["state"],"actual_terminal_error":value.get("error")}),
                );
            }
        }
    }
}

struct ProgressFailures(Vec<anyhow::Error>);
impl std::fmt::Debug for ProgressFailures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProgressFailures")
            .field("actual_source_count", &self.0.len())
            .finish()
    }
}
impl std::fmt::Display for ProgressFailures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "client progress failed with {} retained actual sources",
            self.0.len()
        )
    }
}
impl std::error::Error for ProgressFailures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.first().map(|error| error.as_ref())
    }
}
fn await_saturated_root(
    context: &mut ScenarioContext,
    deadline: Instant,
    epoch: Instant,
    phase: &str,
    samples: &mut Vec<Value>,
    mut assert_held: impl FnMut() -> Result<()>,
) -> Result<()> {
    // Leave the original actor clock available to release gates and capture its
    // actual terminal response when state observation cannot reach the trigger.
    let deadline = deadline.min(Instant::now() + Duration::from_secs(30));
    let ports = context
        .handle()
        .runtime()
        .be
        .iter()
        .map(|be| be.http)
        .collect::<Vec<_>>();
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(1))
        .build()?;
    let mut consecutive = 0;
    loop {
        context.remaining("held root saturation")?;
        ensure!(
            Instant::now() < deadline,
            "root window never saturated before test execution deadline"
        );
        assert_held()?;
        let roots = ports
            .iter()
            .map(|port| -> Result<_> {
                let rows: Value = client
                    .get(format!("http://127.0.0.1:{port}/metrics?type=json"))
                    .send()?
                    .error_for_status()?
                    .json()?;
                root_census(rows.as_array().context("invalid root metric array")?)
            })
            .collect::<Result<Vec<_>>>()?;
        let occupied = roots
            .iter()
            .filter_map(Option::as_ref)
            .filter(|root| root["channels"] != 0)
            .collect::<Vec<_>>();
        // RootChannel refuses another builder when the Data position window is
        // full. Completed rows legally flush partial segments, so retained bytes
        // need not reach the independent byte ceiling. A live producer without
        // End distinguishes this held state from ordinary completed output.
        let saturated = roots.iter().all(Option::is_some)
            && occupied.len() == 1
            && occupied[0]["channels"] == 1
            && occupied[0]["data_positions"] == RootProfileV1::DATA_POSITIONS as u64
            && occupied[0]["payload_bytes"] > 0
            && occupied[0]["payload_bytes"]
                <= (RootProfileV1::DATA_POSITIONS * RootProfileV1::SEGMENT_BYTES) as u64
            && occupied[0]["segments"] >= RootProfileV1::DATA_POSITIONS as u64
            && occupied[0]["retained_reservations"] >= RootProfileV1::DATA_POSITIONS as u64
            && occupied[0]["producers_running"] == 1
            && occupied[0]["producers_exited"] == 0
            && occupied[0]["ends_published"] == 0
            && occupied[0]["ends_acknowledged"] == 0
            && occupied[0]["sealed"] == 0;
        samples.push(json!({"phase":phase,"elapsed_micros":epoch.elapsed().as_micros(),"roots":roots,"saturated":saturated}));
        std::fs::write(
            context
                .scenario_root()
                .join("client-progress-root-census.json"),
            serde_json::to_vec_pretty(samples)?,
        )?;
        consecutive = if saturated { consecutive + 1 } else { 0 };
        if consecutive == 2 {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}
