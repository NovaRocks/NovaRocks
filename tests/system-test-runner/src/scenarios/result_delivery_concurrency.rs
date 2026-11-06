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

//! Four original bounded consumers with separate first-data barriers, exact
//! client/ordinal values, original query cancellation and actual reader joins.

use super::result_delivery_support::await_idle;
use crate::actors::sql::{self, SqlSession};
use crate::actors::trino_protocol::{RawResponse, TrinoClient};
use crate::scenario::{Scenario, ScenarioContext};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::TestClusterOwner;
use novarocks_test_trino_client::{Column, StatementFailure, TrinoError, TypeArgument};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{Arc, Mutex, mpsc},
    time::{Duration, Instant},
};
use tokio::task::{Id, JoinError, JoinSet};

const CLIENTS: usize = 4;
const ROWS: usize = 4096;
const SUFFIX_BYTES: usize = 512;
const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;

pub(super) fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![Box::new(ConcurrentResults)]
}
struct ConcurrentResults;
#[derive(Clone)]
struct OriginalQuery {
    client: usize,
    actor: TrinoClient,
    id: String,
    current_uri: Arc<Mutex<Option<String>>>,
}
#[derive(Serialize)]
struct FirstData {
    client: usize,
    query_id: String,
    rows: usize,
    value_bytes: u64,
    observed_us: u128,
}
#[derive(Serialize)]
struct ActorReport {
    client: usize,
    query_id: String,
    rows: usize,
    value_bytes: u64,
    sha256: String,
    first_get_us: u128,
    first_data_us: Option<u128>,
    resumed_us: Option<u128>,
    terminal_us: Option<u128>,
    columns_observed: bool,
    pages: Vec<Value>,
    actual_public_error: Option<Value>,
}
struct ActorOutcome {
    report: ActorReport,
    failure: Option<anyhow::Error>,
}
type ActualJoin = Option<std::result::Result<(Id, ActorOutcome), JoinError>>;
enum BarrierEvent {
    Data(Option<FirstData>),
    Exit(Box<ActualJoin>),
}

impl Scenario for ConcurrentResults {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "result_delivery_concurrency",
            self.name(),
            true,
            false,
        )
    }
    fn name(&self) -> &'static str {
        "result-delivery/concurrent-results-drain"
    }
    fn is_explicit_stage(&self) -> bool {
        true
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "concurrent results require native 1FE+3BE"
        );
        let epoch = Instant::now();
        await_idle(context, "concurrent-results", "before", epoch)?;
        let deadline = Instant::now()
            + context
                .remaining("concurrent result actors")?
                .min(Duration::from_secs(90));
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(CLIENTS)
            .max_blocking_threads(CLIENTS)
            .enable_all()
            .build()?;
        let mut leases = Vec::new();
        let mut failures = Vec::new();
        let mut submitted = Vec::new();
        // POST does not admit execution. Every original submission and query
        // identity is recorded before any actor can issue its first GET.
        for client in 0..CLIENTS {
            let setup = (|| -> Result<OriginalQuery> {
                let actor = TrinoClient::new(
                    context.trino_port(),
                    context.sql_user(),
                    deadline,
                    &context
                        .scenario_root()
                        .join(format!("concurrent-client-{client}-http")),
                )?;
                let query = format!(
                    "SELECT CONCAT('client-{client}:', CAST(generate_series AS VARCHAR), ':', REPEAT('x', {SUFFIX_BYTES})) AS payload FROM generate_series(1, {ROWS}) ORDER BY generate_series"
                );
                let post = actor.submit(&query)?;
                ensure!(
                    post.status == 200,
                    "concurrent POST did not return an original pending query"
                );
                let value = post.json()?;
                let id = value["id"]
                    .as_str()
                    .context("concurrent POST omitted query identity")?
                    .to_owned();
                let uri = post
                    .next_uri()?
                    .context("concurrent POST omitted its initial continuation")?;
                Ok(OriginalQuery {
                    client,
                    actor,
                    id,
                    current_uri: Arc::new(Mutex::new(Some(uri))),
                })
            })();
            match setup {
                Ok(lease) => {
                    submitted.push(json!({"client":client,"query_id":lease.id,"submitted_us":epoch.elapsed().as_micros()}));
                    leases.push(lease);
                }
                Err(error) => {
                    failures.push(error);
                    break;
                }
            }
        }
        if leases
            .iter()
            .map(|lease| &lease.id)
            .collect::<BTreeSet<_>>()
            .len()
            != leases.len()
        {
            failures.push(anyhow::anyhow!(
                "independent concurrent submissions shared a query identity"
            ));
        }
        let all_submitted_us = epoch.elapsed().as_micros();
        let ReaderGroup {
            mut failures,
            first_data,
            outcomes,
            joins,
            cancellation,
            actors_spawned,
            unjoined_original_handles,
            barrier_completed_us,
            resume_events,
        } = drive_original_readers(
            &runtime,
            &leases,
            failures,
            deadline,
            epoch,
            all_submitted_us,
        );
        if let Err(error) = await_idle(context, "concurrent-results", "after", epoch) {
            failures.push(error);
        }
        let health = (|| -> Result<Value> {
            let deadline = Instant::now()
                + context
                    .remaining("concurrent post-drain health")?
                    .min(Duration::from_secs(15));
            let mut session = SqlSession::new(context.sql_user(), context.trino_port(), deadline)?;
            let result = session.execute("SELECT 1 AS value")?;
            let actual = sql::single_integer(&result)?;
            ensure!(
                actual == 1
                    && result.columns.len() == 1
                    && result.columns[0].type_signature.raw_type == "bigint",
                "concurrent post-drain health lost its original typed value"
            );
            Ok(
                json!({"query_id":result.query_id,"columns":result.columns,"value":actual,"pages":result.pages}),
            )
        })();
        let health = match health {
            Ok(value) => value,
            Err(error) => {
                failures.push(error);
                Value::Null
            }
        };
        if let Err(error) = await_idle(context, "concurrent-results", "after-health", epoch) {
            failures.push(error);
        }
        let receipt = json!({"schema_version":2,"clients":CLIENTS,"rows_per_client":ROWS,"suffix_bytes":SUFFIX_BYTES,
            "execution_budget_seconds":90,"all_submitted_us":all_submitted_us,"submissions":submitted,
            "first_data":first_data,"barrier_completed_us":barrier_completed_us,"resumes":resume_events,
            "actor_outcomes":outcomes,"actual_joins":joins,"original_cancellation":cancellation,
            "actors_spawned":actors_spawned,"unjoined_original_handles":unjoined_original_handles,"post_drain_health":health,
            "actual_failure_sources":failures.len(),"scope":"Bounded per-page reads, literal client/ordinal rows and actual original blocking-reader joins. MySQL packets/metadata sequencing/lenenc digest retire; no whole-query result collector or connection-only control."});
        let stored = serde_json::to_vec_pretty(&receipt)
            .map_err(anyhow::Error::from)
            .and_then(|bytes| {
                std::fs::write(
                    context.scenario_root().join("concurrent-results.json"),
                    bytes,
                )
                .map_err(anyhow::Error::from)
            });
        if let Err(error) = stored {
            failures.push(error);
        }
        if !failures.is_empty() {
            return Err(anyhow::Error::new(ConcurrentFailures(failures)));
        }
        context.action("four independently submitted original queries reached real first-data barriers, retained exact ordered rows across pages, and all original readers and query owners actually exited");
        Ok(())
    }
}

struct ReaderGroup {
    failures: Vec<anyhow::Error>,
    first_data: BTreeMap<usize, FirstData>,
    outcomes: Vec<ActorReport>,
    joins: Vec<Value>,
    cancellation: Vec<Value>,
    actors_spawned: usize,
    unjoined_original_handles: usize,
    barrier_completed_us: Option<u128>,
    resume_events: Vec<Value>,
}

fn drive_original_readers(
    runtime: &tokio::runtime::Runtime,
    leases: &[OriginalQuery],
    mut failures: Vec<anyhow::Error>,
    deadline: Instant,
    epoch: Instant,
    all_submitted_us: u128,
) -> ReaderGroup {
    let (ready, mut readiness) = tokio::sync::mpsc::channel(CLIENTS);
    let mut resumes = Vec::new();
    let mut jobs = JoinSet::new();
    let mut task_owners = HashMap::new();
    if failures.is_empty() {
        for lease in leases {
            let (resume, resume_read) = mpsc::channel();
            resumes.push((lease.client, resume));
            let original = lease.clone();
            let ready = ready.clone();
            let handle = jobs.spawn_blocking_on(
                move || read_original(original, ready, resume_read, deadline, epoch),
                runtime.handle(),
            );
            task_owners.insert(handle.id(), lease.client);
        }
    }
    drop(ready);
    let actors_spawned = task_owners.len();
    let mut first_data = BTreeMap::new();
    let mut outcomes = Vec::new();
    let mut joins = Vec::new();
    let mut cancellation = Vec::new();
    let mut cancel_started = false;
    while failures.is_empty() && first_data.len() < CLIENTS {
        let event = runtime.block_on(async {
            tokio::time::timeout_at(deadline.into(), async {
                tokio::select! {
                    data=readiness.recv()=>BarrierEvent::Data(data),
                    joined=jobs.join_next_with_id()=>BarrierEvent::Exit(Box::new(joined)),
                }
            })
            .await
        });
        match event {
            Ok(BarrierEvent::Data(Some(data))) => {
                let valid = leases
                    .get(data.client)
                    .is_some_and(|lease| lease.id == data.query_id)
                    && data.rows > 0
                    && data.rows <= ROWS
                    && data.observed_us >= all_submitted_us;
                if !valid || first_data.contains_key(&data.client) {
                    failures.push(anyhow::anyhow!(
                        "first-data barrier has a foreign or duplicate original actor"
                    ));
                }
                first_data.insert(data.client, data);
            }
            Ok(BarrierEvent::Data(None)) => failures.push(anyhow::anyhow!(
                "original readers closed before all first-data barriers"
            )),
            Ok(BarrierEvent::Exit(joined)) => {
                record_join(
                    *joined,
                    &mut task_owners,
                    &mut outcomes,
                    &mut joins,
                    &mut failures,
                    epoch,
                );
                if failures.is_empty() {
                    failures.push(anyhow::anyhow!(
                        "original reader exited before all first-data barriers"
                    ));
                }
            }
            Err(error) => failures
                .push(anyhow::Error::new(error).context("original concurrent first-data deadline")),
        }
    }
    let barrier_completed_us = failures.is_empty().then(|| epoch.elapsed().as_micros());
    if !failures.is_empty() {
        cancel_originals(leases, &mut cancellation, &mut failures, epoch);
        cancel_started = true;
    }
    let mut resume_events = Vec::new();
    for (client, resume) in resumes {
        let sent = resume.send(());
        resume_events.push(
            json!({"client":client,"sent_us":epoch.elapsed().as_micros(),"accepted":sent.is_ok()}),
        );
        if let Err(error) = sent {
            failures.push(anyhow::Error::new(error).context("resume original concurrent reader"));
        }
    }
    if !failures.is_empty() && !cancel_started {
        cancel_originals(leases, &mut cancellation, &mut failures, epoch);
        cancel_started = true;
    }
    // Observe whichever original actor exits first. A failure cancels all
    // still-live original query identities before joining another peer.
    // Blocking jobs are never declared exited by abort or observation timeout.
    while !jobs.is_empty() {
        let joined = runtime.block_on(jobs.join_next_with_id());
        record_join(
            joined,
            &mut task_owners,
            &mut outcomes,
            &mut joins,
            &mut failures,
            epoch,
        );
        if !failures.is_empty() && !cancel_started {
            cancel_originals(leases, &mut cancellation, &mut failures, epoch);
            cancel_started = true;
        }
    }
    if !task_owners.is_empty() || joins.len() != actors_spawned {
        failures.push(anyhow::anyhow!(
            "original concurrent actor handles were not all actually joined"
        ));
    }
    if failures.is_empty() {
        let check = (|| -> Result<()> {
            ensure!(
                actors_spawned == CLIENTS
                    && first_data.len() == CLIENTS
                    && outcomes.len() == CLIENTS,
                "finite original concurrent actor set was incomplete"
            );
            let barrier =
                barrier_completed_us.context("original first-data barrier never completed")?;
            for report in &outcomes {
                ensure!(
                    report.first_get_us >= all_submitted_us
                        && report.first_data_us.is_some_and(|at| at <= barrier)
                        && report.resumed_us.is_some_and(|at| at >= barrier)
                        && report.terminal_us.is_some()
                        && report.rows == ROWS
                        && report.value_bytes > 1024 * 1024
                        && report.columns_observed,
                    "concurrent reader did not preserve its original barrier/result/exit order"
                );
            }
            Ok(())
        })();
        if let Err(error) = check {
            failures.push(error);
        }
    }
    ReaderGroup {
        failures,
        first_data,
        outcomes,
        joins,
        cancellation,
        actors_spawned,
        unjoined_original_handles: task_owners.len(),
        barrier_completed_us,
        resume_events,
    }
}

fn read_original(
    lease: OriginalQuery,
    ready: tokio::sync::mpsc::Sender<FirstData>,
    resume: mpsc::Receiver<()>,
    deadline: Instant,
    epoch: Instant,
) -> ActorOutcome {
    let mut report = ActorReport {
        client: lease.client,
        query_id: lease.id.clone(),
        rows: 0,
        value_bytes: 0,
        sha256: String::new(),
        first_get_us: epoch.elapsed().as_micros(),
        first_data_us: None,
        resumed_us: None,
        terminal_us: None,
        columns_observed: false,
        pages: Vec::new(),
        actual_public_error: None,
    };
    let mut hash = Sha256::new();
    let result = (|| -> Result<()> {
        loop {
            let uri = lease
                .current_uri
                .lock()
                .map_err(|_| anyhow::anyhow!("original concurrent URI owner poisoned"))?
                .clone()
                .context("original concurrent query lost its live continuation")?;
            let raw = lease.actor.get(&uri)?;
            let next = consume_page(&raw, &mut report, &mut hash)?;
            *lease
                .current_uri
                .lock()
                .map_err(|_| anyhow::anyhow!("original concurrent URI owner poisoned"))? =
                next.clone();
            if report.first_data_us.is_none() && report.rows > 0 {
                ensure!(
                    next.is_some(),
                    "first data page completed without its terminal continuation"
                );
                let observed = epoch.elapsed().as_micros();
                report.first_data_us = Some(observed);
                ready
                    .blocking_send(FirstData {
                        client: lease.client,
                        query_id: lease.id.clone(),
                        rows: report.rows,
                        value_bytes: report.value_bytes,
                        observed_us: observed,
                    })
                    .context("publish original first-data barrier")?;
                let remaining = deadline
                    .checked_duration_since(Instant::now())
                    .context("original concurrent read gate deadline expired")?;
                resume
                    .recv_timeout(remaining)
                    .context("original concurrent reader resume gate")?;
                report.resumed_us = Some(epoch.elapsed().as_micros());
            }
            if next.is_none() {
                report.terminal_us = Some(epoch.elapsed().as_micros());
                break;
            }
        }
        ensure!(
            report.rows == ROWS && report.columns_observed && report.value_bytes > 1024 * 1024,
            "concurrent result differs from its complete original declaration"
        );
        Ok(())
    })();
    report.sha256 = format!("{:x}", hash.finalize());
    ActorOutcome {
        report,
        failure: result.err(),
    }
}
fn consume_page(
    raw: &RawResponse,
    report: &mut ActorReport,
    hash: &mut Sha256,
) -> Result<Option<String>> {
    ensure!(
        raw.status == 200 && raw.body.len() <= MAX_BODY_BYTES,
        "concurrent page exceeds original raw HTTP status/body contract"
    );
    let value = raw.json()?;
    ensure!(
        value["id"] == report.query_id,
        "concurrent page belongs to another original query"
    );
    if value.get("error").is_some() {
        let fields: BTreeMap<String, Box<serde_json::value::RawValue>> =
            serde_json::from_slice(&raw.body)?;
        let error: TrinoError = serde_json::from_str(
            fields
                .get("error")
                .context("actual concurrent error disappeared")?
                .get(),
        )?;
        report.actual_public_error = Some(serde_json::to_value(&error)?);
        return Err(anyhow::Error::new(StatementFailure::Trino(error)));
    }
    ensure!(
        value.get("updateType").is_none() && value.get("updateCount").is_none(),
        "concurrent rows became command completion"
    );
    if let Some(columns) = value.get("columns") {
        let columns: Vec<Column> = serde_json::from_value(columns.clone())?;
        ensure!(
            columns.len() == 1
                && columns[0].name == "payload"
                && columns[0].type_name == "varchar"
                && columns[0].type_signature.raw_type == "varchar"
                && columns[0].type_signature.arguments == [TypeArgument::Long(2_147_483_647)],
            "concurrent payload lost its exact VARCHAR layout"
        );
        report.columns_observed = true;
    }
    let before = report.rows;
    if let Some(rows) = value.get("data") {
        ensure!(
            report.columns_observed,
            "concurrent rows preceded their public type declaration"
        );
        for row in rows
            .as_array()
            .context("concurrent data is not complete rows")?
        {
            let row = row
                .as_array()
                .context("concurrent row is not a JSON array")?;
            ensure!(
                row.len() == 1 && report.rows < ROWS,
                "concurrent actor received extra cells or rows"
            );
            let cell = row[0]
                .as_str()
                .context("concurrent payload is not non-null VARCHAR")?;
            let prefix = format!("client-{}:{}:", report.client, report.rows + 1);
            let suffix = cell
                .strip_prefix(&prefix)
                .context("concurrent row lost its independent client/ordinal prefix")?;
            ensure!(
                suffix.len() == SUFFIX_BYTES && suffix.bytes().all(|byte| byte == b'x'),
                "concurrent row changed its independent suffix bytes"
            );
            hash.update((cell.len() as u64).to_le_bytes());
            hash.update(cell.as_bytes());
            report.rows += 1;
            report.value_bytes += cell.len() as u64;
        }
    }
    let next = raw.next_uri()?;
    if next.is_none() {
        ensure!(
            value["stats"]["state"] == "FINISHED" && value.get("data").is_none(),
            "concurrent query omitted its separate successful terminal page"
        );
    }
    report.pages.push(json!({"body_bytes":raw.body.len(),"body_sha256":format!("{:x}",Sha256::digest(&raw.body)),
        "rows":report.rows-before,"prefix_rows":report.rows,"has_next_uri":next.is_some(),"state":value["stats"]["state"]}));
    Ok(next)
}
fn record_join(
    joined: ActualJoin,
    owners: &mut HashMap<Id, usize>,
    outcomes: &mut Vec<ActorReport>,
    joins: &mut Vec<Value>,
    failures: &mut Vec<anyhow::Error>,
    epoch: Instant,
) {
    match joined {
        Some(Ok((id, outcome))) => {
            let original = owners.remove(&id);
            if original != Some(outcome.report.client) {
                failures.push(anyhow::anyhow!(
                    "actual concurrent Join has a foreign original task owner"
                ));
            }
            joins.push(json!({"client":original,"reader_task_id":format!("{id:?}"),"joined_us":epoch.elapsed().as_micros(),"panicked":false,"actor_failed":outcome.failure.is_some()}));
            if let Some(error) = outcome.failure {
                failures.push(error);
            }
            outcomes.push(outcome.report);
        }
        Some(Err(error)) => {
            let id = error.id();
            let original = owners.remove(&id);
            joins.push(json!({"client":original,"reader_task_id":format!("{id:?}"),"joined_us":epoch.elapsed().as_micros(),"panicked":error.is_panic(),"cancelled":error.is_cancelled()}));
            failures.push(anyhow::Error::new(error));
        }
        None => failures.push(anyhow::anyhow!(
            "original concurrent JoinSet lost an actor handle"
        )),
    }
}
fn cancel_originals(
    leases: &[OriginalQuery],
    facts: &mut Vec<Value>,
    failures: &mut Vec<anyhow::Error>,
    epoch: Instant,
) {
    for lease in leases {
        let uri = match lease.current_uri.lock() {
            Ok(uri) => uri.clone(),
            Err(poisoned) => {
                // Stand down using only the original stored capability. The
                // poisoning remains a failure; its actual panic stays in JoinError.
                failures.push(anyhow::anyhow!(
                    "original concurrent URI owner was poisoned during cleanup"
                ));
                poisoned.into_inner().clone()
            }
        };
        if let Some(uri) = uri {
            let started = epoch.elapsed().as_micros();
            let reply = lease.actor.delete(&uri);
            let status = reply.as_ref().ok().map(|reply| reply.status);
            facts.push(json!({"client":lease.client,"query_id":lease.id,"started_us":started,"returned_us":epoch.elapsed().as_micros(),"actual_status":status}));
            match reply {
                Ok(reply) if reply.status == 204 => {}
                Ok(_) => failures.push(anyhow::anyhow!(
                    "original concurrent peer DELETE did not return accepted status"
                )),
                Err(error) => failures.push(error.context("cancel original concurrent query")),
            }
        }
    }
}
struct ConcurrentFailures(Vec<anyhow::Error>);
impl std::fmt::Debug for ConcurrentFailures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConcurrentFailures")
            .field("actual_source_count", &self.0.len())
            .finish()
    }
}
impl std::fmt::Display for ConcurrentFailures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "concurrent results failed with {} retained actual sources",
            self.0.len()
        )
    }
}
impl std::error::Error for ConcurrentFailures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.first().map(|error| error.as_ref())
    }
}
#[cfg(test)]
#[path = "result_delivery_concurrency_tests.rs"]
mod tests;
