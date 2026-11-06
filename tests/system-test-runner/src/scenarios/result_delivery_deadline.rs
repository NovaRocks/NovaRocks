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

//! Original execution deadlines, HTTP polling/body clocks and actual owner
//! exits remain separate facts. No request can renew the original query budget.

use super::result_delivery::root_census;
use super::result_delivery_support::await_idle;
use crate::actors::sql::{self, SqlSession};
use crate::actors::trino_protocol::{RawResponse, TrinoClient};
use crate::scenario::{Scenario, ScenarioContext};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::TestClusterOwner;
use novarocks_test_trino_client::{Column, StatementFailure, TrinoError};
use reqwest::blocking::Client;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

const PRODUCER_SQL: &str = "SELECT SLEEP(35) AS value";
const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;
#[derive(Clone, Copy)]
enum Mode {
    ProducerWait,
    QueryDeadline,
}
struct ResponseDeadline(Mode);
pub(super) fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(ResponseDeadline(Mode::ProducerWait)),
        Box::new(ResponseDeadline(Mode::QueryDeadline)),
    ]
}
impl Scenario for ResponseDeadline {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "result_delivery_deadline",
            self.name(),
            true,
            false,
        )
    }
    fn name(&self) -> &'static str {
        match self.0 {
            Mode::ProducerWait => "result-delivery/producer-wait-query-deadline-separation",
            Mode::QueryDeadline => "result-delivery/original-query-deadline-cancels-producer-wait",
        }
    }
    fn is_explicit_stage(&self) -> bool {
        true
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "response deadlines require native 1FE+3BE"
        );
        let epoch = Instant::now();
        await_idle(context, "response-deadline", "before", epoch)?;
        await_actual_exit(context, "before", epoch)?;
        let timeout_seconds = match self.0 {
            Mode::ProducerWait => 120,
            Mode::QueryDeadline => 2,
        };
        let mut actor = TrinoClient::new(
            context.trino_port(),
            context.sql_user(),
            context.deadline(),
            &context.scenario_root().join("response-deadline-http"),
        )?;
        let mut reader = DeadlineRead::default();
        let mut live_uri = None;
        let mut failures = Vec::new();
        let mut heads = Vec::new();
        let mut query_sent_us = None;
        let mut public_terminal_us = None;
        let mut native_before = Vec::new();
        let mut native_after = Vec::new();
        let mut terminal_resources = Value::Null;
        let mut health = Value::Null;
        let mut cleanup_cancel = Value::Null;
        let mut set_effect = Value::Null;
        let result = (|| -> Result<()> {
            set_effect = set_query_timeout(&mut actor, timeout_seconds, &mut live_uri)?;
            native_before = (0..3)
                .map(|backend| {
                    context
                        .handle()
                        .backend_task_execution_tasks_created(backend)
                })
                .collect::<Result<Vec<_>>>()?;
            query_sent_us = Some(epoch.elapsed().as_micros());
            let post = actor.submit(PRODUCER_SQL)?;
            reader.id = post.json()?["id"]
                .as_str()
                .context("SLEEP POST omitted its original query identity")?
                .to_owned();
            live_uri = post.next_uri()?;
            live_uri = reader.observe(&post, self.0, false)?;
            drop(post);
            let mut first_get_completed = false;
            while let Some(uri) = live_uri.clone() {
                // HEAD occurs only after actual GET admission and an original
                // empty page. Its body is never treated as first data or exit.
                if matches!(self.0, Mode::QueryDeadline)
                    && first_get_completed
                    && reader.empty_gets > 0
                {
                    for _ in 0..2 {
                        let before = epoch.elapsed().as_micros();
                        let reply = actor.head(&uri)?;
                        ensure!(
                            reply.status == 200 && reply.body.is_empty(),
                            "original active-query HEAD changed its status/body contract"
                        );
                        heads.push(json!({"started_us":before,"returned_us":epoch.elapsed().as_micros(),"status":reply.status,"body_bytes":reply.body.len()}));
                    }
                }
                let page = actor.get(&uri)?;
                live_uri = reader.observe(&page, self.0, true)?;
                first_get_completed = true;
            }
            public_terminal_us = Some(epoch.elapsed().as_micros());
            match self.0 {
                Mode::ProducerWait => ensure!(
                    reader.value == Some(true) && reader.public_error.is_none() && reader.finished,
                    "original SLEEP(35) did not publish exactly one BOOLEAN true and terminal success"
                ),
                Mode::QueryDeadline => ensure!(
                    reader.value.is_none()
                        && reader.public_error.is_some()
                        && reader.empty_gets > 0
                        && heads.len() >= 2,
                    "original deadline was not observed after active empty-page/HEAD polling"
                ),
            }
            native_after = (0..3)
                .map(|backend| {
                    context
                        .handle()
                        .backend_task_execution_tasks_created(backend)
                })
                .collect::<Result<Vec<_>>>()?;
            ensure!(
                native_before
                    .iter()
                    .chain(&native_after)
                    .all(|count| count.is_finite() && *count >= 0.0 && count.fract() == 0.0)
                    && native_after.iter().sum::<f64>() > native_before.iter().sum::<f64>(),
                "original producer query did not create actual Native work"
            );
            let snapshot = context
                .handle()
                .query_execution_resource_snapshot()?
                .context("post-response original resource snapshot unavailable")?;
            terminal_resources = json!({"fe_running":snapshot.fe_running,"backends":snapshot.backends.iter().map(|backend|
                json!({"index":backend.index,"process_running":backend.process_running,"resources":backend.resources})).collect::<Vec<_>>()});
            // Preserve ordinary progress after this response. Connection-only
            // same-socket identity is retired; no new query renews the old one.
            let mut session =
                SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
            let result = session.execute("SELECT 1")?;
            let actual = sql::single_integer(&result)?;
            ensure!(
                actual == 1
                    && result.columns.len() == 1
                    && result.columns[0].type_signature.raw_type == "bigint",
                "ordinary query after the deadline response changed its typed value"
            );
            health = json!({"query_id":result.query_id,"pages":result.pages,"columns":result.columns,"value":actual,"observed_us":epoch.elapsed().as_micros()});
            Ok(())
        })();
        if let Err(error) = result {
            failures.push(error);
        }
        if !failures.is_empty()
            && let Some(uri) = live_uri.as_deref()
        {
            let started = epoch.elapsed().as_micros();
            let cancelled = actor.delete(uri);
            cleanup_cancel = json!({"started_us":started,"returned_us":epoch.elapsed().as_micros(),"actual_status":cancelled.as_ref().ok().map(|reply|reply.status)});
            match cancelled {
                Ok(reply) if reply.status == 204 => {}
                Ok(_) => failures.push(anyhow::anyhow!(
                    "original deadline query cleanup DELETE was not accepted"
                )),
                Err(error) => failures.push(error.context("cancel original failed deadline query")),
            }
        }
        // Requests run synchronously and return their actual body result.
        // Observe Native CPU, producer and transport exit after client Drop.
        drop(actor);
        let client_dropped_us = epoch.elapsed().as_micros();
        if let Err(error) = await_actual_exit(context, "after-client", epoch) {
            failures.push(error);
        }
        if let Err(error) = await_idle(context, "response-deadline", "after-client", epoch) {
            failures.push(error);
        }
        let receipt = json!({"schema_version":2,"scenario":self.name(),"sql":PRODUCER_SQL,"query_timeout_seconds":timeout_seconds,
            "query_id":reader.id,"set_effect":set_effect,"query_sent_us":query_sent_us,"public_terminal_us":public_terminal_us,
            "actual_boolean_value":reader.value,"columns_observed":reader.columns_observed,"actual_server_error":reader.error_wire,
            "expected_server_error":{"errorCode":131075,"errorName":"EXCEEDED_TIME_LIMIT","errorType":"INSUFFICIENT_RESOURCES","original_public_cause":"query.execution.deadline_exceeded"},
            "empty_get_pages":reader.empty_gets,"heads_after_get_admission":heads,"pages":reader.pages,
            "native_before_tasks_created":native_before,"native_after_tasks_created":native_after,
            "actual_resources_at_public_response":terminal_resources,"ordinary_progress":health,"cleanup_cancel":cleanup_cancel,
            "client_dropped_us":client_dropped_us,"actual_exit_barriers_completed_us":epoch.elapsed().as_micros(),"actual_failure_sources":failures.len(),
            "scope":"Original35s producer,120/2 execution deadlines, per-response HTTP write clock and actual owner exit stay separate. Legacy errno1317/SQLSTATE70100/shared text disambiguation and same-socket identity retire; typed Trino deadline is distinct from USER_CANCELED. Context request budget is never renewed."});
        let stored = serde_json::to_vec_pretty(&receipt)
            .map_err(anyhow::Error::from)
            .and_then(|bytes| {
                std::fs::write(
                    context.scenario_root().join("response-deadline.json"),
                    bytes,
                )
                .map_err(anyhow::Error::from)
            });
        if let Err(error) = stored {
            failures.push(error);
        }
        if !failures.is_empty() {
            return Err(anyhow::Error::new(DeadlineFailures(failures)));
        }
        context.action("original execution deadline and actual Native owner exit independently observed; empty GET/HEAD do not renew the query; ordinary typed progress remains");
        Ok(())
    }
}
#[derive(Default)]
struct DeadlineRead {
    id: String,
    value: Option<bool>,
    columns_observed: bool,
    empty_gets: usize,
    finished: bool,
    public_error: Option<TrinoError>,
    error_wire: Option<Value>,
    pages: Vec<Value>,
}
impl DeadlineRead {
    fn observe(&mut self, raw: &RawResponse, mode: Mode, is_get: bool) -> Result<Option<String>> {
        ensure!(
            raw.status == 200 && raw.body.len() <= MAX_BODY_BYTES,
            "original deadline response changed its status/body contract"
        );
        let value = raw.json()?;
        ensure!(
            value["id"] == self.id,
            "original producer response changed query identity"
        );
        let next = raw.next_uri()?;
        let before = self.value;
        if value.get("error").is_some() {
            let error = decode_trino_error(raw)?;
            self.error_wire = Some(serde_json::to_value(&error)?);
            if !matches!(mode, Mode::QueryDeadline)
                || error.error_code != 131075
                || error.error_name != "EXCEEDED_TIME_LIMIT"
                || error.error_type != "INSUFFICIENT_RESOURCES"
            {
                return Err(anyhow::Error::new(StatementFailure::Trino(error)));
            }
            if next.is_some()
                || value["stats"]["state"] != "FAILED"
                || ["columns", "data", "updateType", "updateCount"]
                    .iter()
                    .any(|field| value.get(*field).is_some())
            {
                return Err(anyhow::Error::new(StatementFailure::Trino(error))
                    .context("original deadline error carried a success shape"));
            }
            self.public_error = Some(error);
        } else {
            ensure!(
                value.get("updateType").is_none() && value.get("updateCount").is_none(),
                "original SLEEP rows became command completion"
            );
            if let Some(columns) = value.get("columns") {
                let columns: Vec<Column> = serde_json::from_value(columns.clone())?;
                ensure!(
                    columns.len() == 1
                        && columns[0].name == "value"
                        && columns[0].type_name == "boolean"
                        && columns[0].type_signature.raw_type == "boolean"
                        && columns[0].type_signature.arguments.is_empty(),
                    "SLEEP changed its exact BOOLEAN result declaration"
                );
                self.columns_observed = true;
            }
            if let Some(rows) = value.get("data") {
                ensure!(
                    self.columns_observed,
                    "SLEEP data arrived before its actual public metadata"
                );
                for row in rows.as_array().context("SLEEP data is not complete rows")? {
                    let row = row.as_array().context("SLEEP result is not a JSON row")?;
                    ensure!(
                        row.len() == 1 && self.value.is_none(),
                        "SLEEP published extra rows or cells"
                    );
                    self.value = Some(
                        row[0]
                            .as_bool()
                            .context("SLEEP result is not a non-null BOOLEAN")?,
                    );
                    ensure!(
                        self.value == Some(true),
                        "SLEEP(35) differs from independent BOOLEAN true"
                    );
                }
            }
            if next.is_none() {
                ensure!(
                    value["stats"]["state"] == "FINISHED" && value.get("data").is_none(),
                    "SLEEP omitted its separate successful terminal page"
                );
                self.finished = true;
            }
            if matches!(mode, Mode::QueryDeadline) && self.value.is_some() {
                anyhow::bail!("the original two-second query deadline did not cancel SLEEP(35)");
            }
            if is_get && self.value == before && self.value.is_none() && next.is_some() {
                self.empty_gets += 1;
            }
        }
        self.pages.push(json!({"get":is_get,"body_bytes":raw.body.len(),"body_sha256":format!("{:x}",Sha256::digest(&raw.body)),
            "state":value["stats"]["state"],"has_next_uri":next.is_some(),"empty_get_pages":self.empty_gets,"boolean_value":self.value,"deadline_error":self.public_error.is_some()}));
        Ok(next)
    }
}
fn decode_trino_error(raw: &RawResponse) -> Result<TrinoError> {
    let fields: BTreeMap<String, Box<serde_json::value::RawValue>> =
        serde_json::from_slice(&raw.body)?;
    Ok(serde_json::from_str(
        fields
            .get("error")
            .context("original error object missing")?
            .get(),
    )?)
}

struct DeadlineFailures(Vec<anyhow::Error>);
impl std::fmt::Debug for DeadlineFailures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeadlineFailures")
            .field("actual_source_count", &self.0.len())
            .finish()
    }
}
impl std::fmt::Display for DeadlineFailures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "response deadline scenario failed with {} retained actual sources",
            self.0.len()
        )
    }
}
impl std::error::Error for DeadlineFailures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.first().map(|error| error.as_ref())
    }
}
fn await_actual_exit(context: &mut ScenarioContext, phase: &str, epoch: Instant) -> Result<()> {
    let client = Client::builder().timeout(Duration::from_secs(1)).build()?;
    let mut samples = Vec::new();
    let outcome = (|| -> Result<()> {
        let mut consecutive = 0;
        loop {
            context.remaining("actual response owner exit")?;
            let resource = context
                .handle()
                .query_execution_resource_snapshot()?
                .context("native query execution resource observation is unavailable")?;
            ensure!(
                resource.fe_running && resource.backends.len() == 3,
                "native role processes changed during owner exit observation"
            );
            let mut idle = true;
            let mut backends = Vec::new();
            let ports = context
                .handle()
                .runtime()
                .be
                .iter()
                .map(|be| be.http)
                .collect::<Vec<_>>();
            for (backend, port) in resource.backends.iter().zip(ports) {
                ensure!(
                    backend.process_running && !backend.resources.is_empty(),
                    "BE query owner observation is absent or its process exited"
                );
                for count in backend.resources.values() {
                    ensure!(
                        count.is_finite() && *count >= 0.0 && count.fract() == 0.0,
                        "invalid actual BE owner count"
                    );
                    idle &= *count == 0.0;
                }
                let rows: Value = client
                    .get(format!("http://127.0.0.1:{port}/metrics?type=json"))
                    .send()?
                    .error_for_status()?
                    .json()?;
                let roots = root_census(
                    rows.as_array()
                        .context("invalid root census metric array")?,
                )?;
                // A busy or incomplete census is explicitly unavailable.
                idle &= roots
                    .as_ref()
                    .is_some_and(|counts| counts.values().all(|count| *count == 0));
                backends.push(json!({"index":backend.index,"query_resources":backend.resources,"root_ownership":roots}));
            }
            samples.push(json!({"query_epoch_micros":epoch.elapsed().as_micros(),"idle":idle,"backends":backends}));
            consecutive = if idle { consecutive + 1 } else { 0 };
            if consecutive == 2 {
                return Ok(());
            }
            ensure!(
                samples.len() < 3001,
                "owner exit observation exceeded the original finite sample guard"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    })();
    std::fs::write(
        context
            .scenario_root()
            .join(format!("response-deadline-owner-exit-{phase}.json")),
        serde_json::to_vec_pretty(&json!({"schema_version":1,"phase":phase,"samples":samples,
            "error":outcome.as_ref().err().map(|error| format!("{error:#}"))}))?,
    )?;
    outcome
}

pub(super) fn set_query_timeout(
    actor: &mut TrinoClient,
    timeout_seconds: u64,
    live_uri: &mut Option<String>,
) -> Result<Value> {
    let mut page = actor.submit(&format!("SET query_timeout = {timeout_seconds}"))?;
    *live_uri = page.next_uri()?;
    let set_id = page.json()?["id"]
        .as_str()
        .context("SET omitted its original query identity")?
        .to_owned();
    let terminal = loop {
        let value = page.json()?;
        ensure!(
            value["id"] == set_id,
            "SET changed its original query identity"
        );
        *live_uri = page.next_uri()?;
        if value.get("error").is_some() {
            return Err(anyhow::Error::new(StatementFailure::Trino(
                decode_trino_error(&page)?,
            )));
        }
        ensure!(
            page.status == 200
                && value
                    .get("columns")
                    .is_none_or(|columns| *columns == json!([]))
                && value.get("data").is_none()
                && value.get("updateCount").is_none(),
            "SET changed its original zero-row command shape"
        );
        let Some(uri) = live_uri.as_deref() else {
            break value;
        };
        page = actor.get(uri)?;
    };
    let expected_effect = format!("query_timeout={timeout_seconds}");
    ensure!(
        terminal["stats"]["state"] == "FINISHED"
            && terminal["columns"] == json!([])
            && terminal["updateType"] == "SET SESSION"
            && page
                .headers
                .get_all("x-trino-set-session")
                .iter()
                .any(|value| value == expected_effect.as_str()),
        "successful SET omitted its exact terminal SessionEffect"
    );
    actor.apply_effects(&page)?;
    Ok(json!({"update_type":terminal["updateType"],"actual_set_session":expected_effect}))
}
