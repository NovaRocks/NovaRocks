// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this
// file to you under the Apache License, Version 2.0 (the
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

//! Storage consumers declare one exact authority intent before role startup.
use super::{
    ConnectorRead, VENDED_EXECUTION_CREDENTIAL_GENERATION, VENDED_EXECUTION_CREDENTIAL_NAME,
};
use crate::actors::mysql as mysql_actor;
use crate::scenario::ScenarioContext;
use anyhow::{Context, Result, bail, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::test_sessions::{ClusterTestSessionPlan, RoleSlot};
use novarocks_cluster_harness::{QueryLifecycleStatementObservation, ServerHandle};
use novarocks_failpoint::session::DomainGrant;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::fs;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

pub(super) const REFRESH_HELD_TOKEN: &str = "storage-refresh-held";
pub(super) const FAULT_PRIME_TOKEN: &str = "storage-fault-prime";
pub(super) const FAULT_READ_TOKEN: &str = "storage-fault-read";
pub(super) const FAULT_FOLLOWUP_TOKEN: &str = "storage-fault-followup";

#[derive(Clone)]
pub(super) struct AuthorityIntent {
    pub catalog: String,
    pub scope: String,
    pub endpoint: String,
    pub close: bool,
}
impl AuthorityIntent {
    fn target(&self) -> Value {
        json!({"catalog":self.catalog,"scope":self.scope,
            "principal_name":VENDED_EXECUTION_CREDENTIAL_NAME,
            "principal_generation":VENDED_EXECUTION_CREDENTIAL_GENERATION,
            "capability":"credentials-endpoint","endpoint":self.endpoint})
    }
}
#[derive(Clone)]
pub(super) struct Inputs {
    pub root: PathBuf,
    pub intent: AuthorityIntent,
    pub queries: BTreeMap<String, String>,
}

pub(super) struct QueryIntent {
    pub run_token: &'static str,
    pub sql: String,
    pub lease_markers: bool,
}

pub(super) fn held_sql(catalog: &str, database: &str, table: &str) -> String {
    // Preserve the existing connector cancellation corpus and scan-side sleep.
    format!(
        "SELECT t.v FROM (SELECT v FROM {catalog}.{database}.{table} WHERE v % 4096 = 0 AND sleep(1) = 0) AS t CROSS JOIN TABLE(generate_series(1, 64)) AS gs(x)"
    )
}

pub(super) fn plan(
    cluster_size: usize,
    binary: &Path,
    original_deadline: Instant,
    authority: AuthorityIntent,
    queries: &[QueryIntent],
) -> Result<(ClusterTestSessionPlan, Inputs)> {
    ensure!(
        cluster_size == 3,
        "Storage credential consumers require native 1FE+3BE"
    );
    let mut plan = ClusterTestSessionPlan::new(original_deadline)?;
    let root = plan.reserve_transport("storage-authority")?;
    let bindings = plan.reserve_transport("storage-query-bindings")?;
    let frontend = plan.reserve_launch(RoleSlot::Frontend)?;
    let mut backends = Vec::new();
    for index in 0..cluster_size {
        backends.push(plan.reserve_launch(RoleSlot::Backend(index))?);
    }
    let mut fe_grants = Vec::new();
    let mut be_grants = vec![Vec::new(); cluster_size];
    be_grants[0].extend(authority_grants(&authority, &root));
    if !queries.is_empty() {
        fe_grants.push(DomainGrant {
            domain: "statement-observation".into(),
            grant_id: "storage-exact-statements".into(),
            parameters: json!({"action":"observe","bearer_secret":plan.fresh_domain_secret()?}),
        });
    }
    let mut tokens = std::collections::BTreeSet::new();
    for query in queries {
        ensure!(
            !query.sql.is_empty() && tokens.insert(query.run_token),
            "Storage query targets must be finite and unique"
        );
        for action in if query.lease_markers {
            &["reader-marker", "grpc-fragment"][..]
        } else {
            &["reader-marker"][..]
        } {
            for index in 0..cluster_size {
                fe_grants.push(DomainGrant { domain:"native-observation".into(),grant_id:format!("{}-bind-{action}-{index}",query.run_token),
                    parameters:json!({"action":format!("bind-{action}"),"root":bindings,"backend_index":index,
                        "statement_run_token":query.run_token,"target_session_id":backends[index].session_id,
                        "target_process_receipt":backends[index].process_receipt}) });
                be_grants[index].push(DomainGrant { domain:"native-observation".into(),grant_id:format!("{}-{action}",query.run_token),
                    parameters:json!({"action":action,"root":bindings,"backend_index":index,
                        "statement_run_token":query.run_token,"target_session_id":backends[index].session_id,"max_events":4096}) });
            }
        }
    }
    if fe_grants.is_empty() {
        plan.authorize_ordinary_launch(&frontend, binary.to_path_buf())?;
    } else {
        plan.set_launch_binary(&frontend, binary.to_path_buf())?;
        plan.authorize_launch(
            &frontend,
            fe_grants,
            requirements(&[
                "novarocks-server",
                "novarocks-frontend-application",
                "novarocks-native-adapter",
            ]),
        )?;
    }
    for (index, grants) in be_grants.into_iter().enumerate() {
        if grants.is_empty() {
            plan.authorize_ordinary_launch(&backends[index], binary.to_path_buf())?;
        } else {
            let mut owners = vec![
                "novarocks-server",
                "novarocks-native-adapter",
                "novarocks-worker",
                "novarocks-execution",
            ];
            if index == 0 {
                owners.push("novarocks-fs");
            }
            plan.set_launch_binary(&backends[index], binary.to_path_buf())?;
            plan.authorize_launch(&backends[index], grants, requirements(&owners))?;
        }
    }
    plan.validate_selected_intents(cluster_size)?;
    Ok((
        plan,
        Inputs {
            root,
            intent: authority,
            queries: queries
                .iter()
                .map(|query| (query.run_token.to_string(), query.sql.clone()))
                .collect(),
        },
    ))
}
fn requirements(owners: &[&str]) -> Vec<(String, String)> {
    owners
        .iter()
        .map(|owner| ((*owner).into(), "test-controls".into()))
        .collect()
}

fn authority_grants(authority: &AuthorityIntent, root: &Path) -> Vec<DomainGrant> {
    let actions = if authority.close {
        &["observe", "close"][..]
    } else {
        &["observe"][..]
    };
    actions.iter().map(|action| DomainGrant {
        domain: "storage-authority".into(),
        grant_id: format!("authority-{action}"),
        parameters: json!({"action":action,"root":root,"backend_index":0,"target":authority.target()}),
    }).collect()
}

/// Reserve the actual unique endpoint before declarations. It is closed only
/// after the original setup catalog write, before target catalog acquisition.
pub(super) struct UnreachableEndpoint {
    listener: TcpListener,
    address: SocketAddr,
    pub endpoint: String,
}
impl UnreachableEndpoint {
    pub fn prepare() -> Result<Self> {
        let listener =
            TcpListener::bind("127.0.0.1:0").context("reserve prelaunch credentials endpoint")?;
        let address = listener.local_addr()?;
        let endpoint = format!("http://{address}/_fixture/vended-credentials/refresh");
        Ok(Self {
            listener,
            address,
            endpoint,
        })
    }
    pub fn release_and_verify(self, context: &ScenarioContext) -> Result<String> {
        drop(self.listener);
        let result = TcpStream::connect_timeout(
            &self.address,
            context
                .remaining("verify closed credentials endpoint")?
                .min(Duration::from_secs(1)),
        );
        let refusal = match result {
            Err(error) => error,
            Ok(_) => bail!("closed fixture endpoint unexpectedly accepted a connection"),
        };
        ensure!(
            refusal.kind() == std::io::ErrorKind::ConnectionRefused,
            "closed fixture endpoint did not refuse connections: {refusal}"
        );
        Ok(self.endpoint)
    }
}

pub(super) struct AuthorityIdentity {
    pub key: String,
    pub version: String,
}
pub(super) fn identity(context: &ScenarioContext, inputs: &Inputs) -> Result<AuthorityIdentity> {
    let prefix = "be-0.storage-authority-";
    loop {
        let mut matches = Vec::new();
        for entry in fs::read_dir(&inputs.root)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !name.starts_with(prefix) || !name.ends_with(".identity") {
                continue;
            }
            let contents = fs::read_to_string(entry.path())?;
            let fields = contents
                .lines()
                .filter_map(|line| line.split_once('='))
                .collect::<BTreeMap<_, _>>();
            if fields.get("catalog") != Some(&inputs.intent.catalog.as_str())
                || fields.get("scope") != Some(&inputs.intent.scope.as_str())
                || fields.get("principal") != Some(&"vended-rest-execution:v1")
                || fields.get("path") != Some(&"credentials-endpoint")
            {
                continue;
            }
            let key = fields
                .get("key")
                .context("authority identity omitted its key")?
                .to_string();
            let version = fields
                .get("version")
                .context("authority identity omitted its version")?
                .to_string();
            ensure!(
                [&key, &version].iter().all(|value| value.len() == 64
                    && value
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())),
                "invalid actual authority identity"
            );
            matches.push(AuthorityIdentity { key, version });
        }
        ensure!(
            matches.len() <= 1,
            "ambiguous actual authority identity for frozen intent"
        );
        if let Some(identity) = matches.pop() {
            return Ok(identity);
        }
        thread::sleep(
            context
                .remaining("wait for frozen Storage authority identity")?
                .min(Duration::from_millis(20)),
        );
    }
}
pub(super) fn close(
    context: &ScenarioContext,
    inputs: &Inputs,
    key: &str,
) -> Result<BTreeMap<String, String>> {
    ensure!(
        inputs.intent.close,
        "Storage observation alone does not authorize close"
    );
    let trigger = inputs.root.join("be-0.storage-authority-close.trigger");
    let temporary = trigger.with_extension("trigger.tmp");
    fs::write(&temporary, format!("{key}\n"))?;
    fs::rename(temporary, trigger)?;
    record(
        context,
        &inputs.root.join("be-0.storage-authority-close.confirmed"),
        key,
        "closed",
    )
}
pub(super) fn late(
    context: &ScenarioContext,
    inputs: &Inputs,
    key: &str,
) -> Result<BTreeMap<String, String>> {
    record(
        context,
        &inputs
            .root
            .join("be-0.storage-authority-close.late-confirmed"),
        key,
        "discarded",
    )
}
fn record(
    context: &ScenarioContext,
    path: &Path,
    key: &str,
    status: &str,
) -> Result<BTreeMap<String, String>> {
    loop {
        match fs::read_to_string(path) {
            Ok(contents) => {
                let fields = contents
                    .lines()
                    .filter_map(|line| line.split_once('='))
                    .map(|(key, value)| (key.into(), value.into()))
                    .collect::<BTreeMap<String, String>>();
                ensure!(
                    fields.get("key").is_some_and(|value| value == key)
                        && fields.get("status").is_some_and(|value| value == status),
                    "authority record disagrees with frozen target"
                );
                return Ok(fields);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        thread::sleep(
            context
                .remaining("wait for exact authority close record")?
                .min(Duration::from_millis(20)),
        );
    }
}

pub(super) fn with_cleanup<T>(primary: Result<T>, cleanup: Result<()>) -> Result<T> {
    match (primary, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(_), Err(cleanup)) => Err(cleanup),
        (Err(primary), Err(cleanup)) => {
            Err(anyhow::anyhow!("{primary:#}; cleanup failed: {cleanup:#}"))
        }
    }
}
fn exact_sql(inputs: &Inputs, token: &str, sql: &str) -> Result<()> {
    ensure!(
        inputs
            .queries
            .get(token)
            .is_some_and(|expected| expected == sql),
        "submitted SQL differs from its frozen Storage query intent"
    );
    Ok(())
}
pub(super) fn outcome<T: mysql::prelude::FromRow>(
    context: &mut ScenarioContext,
    inputs: &Inputs,
    conn: &mut mysql::Conn,
    token: &str,
    sql: &str,
) -> Result<std::result::Result<Vec<T>, mysql::Error>> {
    exact_sql(inputs, token, sql)?;
    let deadline = context.deadline();
    let observation = context.handle().arm_selected_statement_observation(
        token,
        conn.connection_id(),
        sql,
        deadline,
    )?;
    let result = conn.query(sql);
    if let Err(cleanup) = context
        .handle()
        .drain_query_lifecycle_statement_observation(&observation)
    {
        let outcome = result
            .as_ref()
            .err()
            .map_or_else(|| "success".to_string(), ToString::to_string);
        bail!("statement outcome: {outcome}; drain failed: {cleanup:#}");
    }
    Ok(result)
}
/// The original SQL result and its exact owner-authored terminal, observed
/// before the same observation is drained. This value grants no control.
pub(super) struct ObservedStorageOutcome<T> {
    pub result: mysql::Result<Vec<T>>,
    pub snapshot: novarocks_cluster_harness::QueryLifecycleStructuredSnapshot,
}

/// A SQL error consumed by a case remains diagnostic input when its new
/// observation or semantic assertion fails. Expected SQL failure alone does
/// not turn an otherwise successful case into failure.
fn with_original_sql_failure<T>(
    original_sql_failure: &mysql::Error,
    outcome: Result<T>,
) -> Result<T> {
    outcome.with_context(|| format!("original Storage SQL failed: {original_sql_failure}"))
}

fn finish_observed_outcome<T>(
    result: mysql::Result<Vec<T>>,
    snapshot: Result<novarocks_cluster_harness::QueryLifecycleStructuredSnapshot>,
    drained: Result<()>,
) -> Result<ObservedStorageOutcome<T>> {
    let snapshot = with_cleanup(snapshot, drained);
    let snapshot = match &result {
        Ok(_) => snapshot.context("original Storage SQL succeeded"),
        Err(error) => with_original_sql_failure(error, snapshot),
    }?;
    Ok(ObservedStorageOutcome { result, snapshot })
}

pub(super) fn observed_outcome<T: mysql::prelude::FromRow>(
    context: &mut ScenarioContext,
    inputs: &Inputs,
    conn: &mut mysql::Conn,
    token: &str,
    sql: &str,
) -> Result<ObservedStorageOutcome<T>> {
    exact_sql(inputs, token, sql)?;
    let deadline = context.deadline();
    let observation = context.handle().arm_selected_statement_observation(
        token,
        conn.connection_id(),
        sql,
        deadline,
    )?;
    let result = conn.query(sql);
    let snapshot = context
        .handle()
        .await_query_lifecycle_statement_observation(&observation, false, deadline);
    let drained = context
        .handle()
        .drain_query_lifecycle_statement_observation(&observation);
    finish_observed_outcome(result, snapshot, drained)
}

pub(super) fn assert_acquisition_failure(
    context: &mut ScenarioContext,
    snapshot: &novarocks_cluster_harness::QueryLifecycleStructuredSnapshot,
    code: u16,
    original_sql_failure: &mysql::Error,
) -> Result<()> {
    let asserted = (|| {
        check_acquisition_failure(context, snapshot, code)?;
        let detail = snapshot
            .failure_detail
            .as_deref()
            .context("Storage acquisition failure omitted its original safe detail")?;
        let server = check_acquisition_mysql_error(original_sql_failure, detail)?;
        context.action(format!(
            "observed original acquisition MySQL error code={} state={} descriptor=query.execution.dependency",
            server.code, server.state
        ));
        Ok(())
    })();
    with_original_sql_failure(original_sql_failure, asserted)
}

/// Cross-check the original server packet against the same statement's
/// owner-authored safe detail. A client transport failure proves no server
/// failure descriptor, even if an independent Native terminal was observed.
fn check_acquisition_mysql_error<'a>(
    error: &'a mysql::Error,
    safe_detail: &str,
) -> Result<&'a mysql::MySqlError> {
    let mysql::Error::MySqlError(server) = error else {
        bail!("Storage acquisition failure did not return an original MySQL server error: {error}");
    };
    ensure!(
        server.code == 1105 && server.state == "HY000",
        "Storage acquisition failure changed its MySQL code/state: code={} state={}",
        server.code,
        server.state
    );
    let expected = format!("[query.execution.dependency] {safe_detail}");
    ensure!(
        server.message == expected,
        "Storage acquisition MySQL message differs from its original safe projection: expected={expected:?} observed={:?}",
        server.message
    );
    Ok(server)
}

fn check_acquisition_failure(
    context: &mut ScenarioContext,
    snapshot: &novarocks_cluster_harness::QueryLifecycleStructuredSnapshot,
    code: u16,
) -> Result<()> {
    use novarocks_cluster_harness::QueryLifecycleFailureSite;
    use novarocks_execution_contract::{
        TaskFailurePhase,
        failure::{FailureClass, FailureFact, FailureReason, domain},
    };
    use std::num::NonZeroU16;
    let expected = FailureFact::new(
        FailureClass::Dependency,
        FailureReason::new(
            domain::STORAGE_AUTHORITY,
            NonZeroU16::new(code).context("acquisition reason must be nonzero")?,
        ),
    );
    ensure!(
        snapshot.failure_fact == Some(expected),
        "Storage statement lost its acquisition cause: {snapshot:?}"
    );
    ensure!(
        snapshot.worker_contexts_converged,
        "Storage failure was not an original converged attempt: {snapshot:?}"
    );
    let Some(QueryLifecycleFailureSite::Task { identity, phase }) = &snapshot.failure_site else {
        bail!("Storage acquisition failure has no original Task site: {snapshot:?}");
    };
    ensure!(
        *phase == TaskFailurePhase::Execution,
        "Storage failure came from another Task phase: {snapshot:?}"
    );
    ensure!(
        identity.backend_process_id() == context.handle().backend_process_id(0)?,
        "Storage failure did not originate on the targeted BE[0]: {snapshot:?}"
    );
    let execution = identity.query_execution_id();
    let expected_execution = format!(
        "{}:{}:{}",
        execution.query_id().high(),
        execution.query_id().low(),
        execution.attempt_id().get()
    );
    ensure!(
        snapshot.execution_id.as_deref() == Some(expected_execution.as_str())
            && snapshot.attempt_id == execution.attempt_id().get(),
        "Storage failure belongs to another execution/attempt: {snapshot:?}"
    );
    context.action(format!("observed original acquisition failure reason=23:{code} execution={expected_execution} site={:?}", snapshot.failure_site));
    Ok(())
}

pub(super) fn query<T: mysql::prelude::FromRow>(
    context: &mut ScenarioContext,
    inputs: &Inputs,
    conn: &mut mysql::Conn,
    token: &str,
    sql: &str,
) -> Result<Vec<T>> {
    outcome(context, inputs, conn, token, sql)?.map_err(anyhow::Error::from)
}

pub(super) struct SimpleRead {
    pub result: mpsc::Receiver<std::result::Result<Vec<i64>, mysql::Error>>,
    thread: thread::JoinHandle<Result<()>>,
    observation: Option<QueryLifecycleStatementObservation>,
    connection_id: u32,
}
impl SimpleRead {
    pub fn start(
        context: &mut ScenarioContext,
        inputs: &Inputs,
        user: &str,
        port: u16,
        token: &str,
        sql: &str,
    ) -> Result<Self> {
        exact_sql(inputs, token, sql)?;
        let conn = mysql_actor::connect_for_cancellation_until(user, port, context.deadline())?;
        let deadline = context.deadline();
        let connection_id = conn.connection_id();
        let observation = context.handle().arm_selected_statement_observation(
            token,
            connection_id,
            sql,
            deadline,
        )?;
        let sql = sql.to_owned();
        let (sender, result) = mpsc::sync_channel(1);
        let worker = thread::Builder::new()
            .name("storage-exact-read".into())
            .spawn(move || {
                conn.run(|conn| {
                    mysql_actor::publish_result(&sender, conn.query(sql))
                        .context("publish exact Storage read result")?;
                    Ok(())
                })
            });
        match worker {
            Ok(thread) => Ok(Self {
                result,
                thread,
                observation: Some(observation),
                connection_id,
            }),
            Err(error) => {
                let drained = context
                    .handle()
                    .drain_query_lifecycle_statement_observation(&observation);
                with_cleanup(Err(error.into()), drained)
            }
        }
    }
    /// This read has no marker oracle and consumes no statement/action grant.
    pub fn start_ordinary(
        context: &ScenarioContext,
        user: &str,
        port: u16,
        sql: &str,
    ) -> Result<Self> {
        let conn = mysql_actor::connect_for_cancellation_until(user, port, context.deadline())?;
        let connection_id = conn.connection_id();
        let sql = sql.to_owned();
        let (sender, result) = mpsc::sync_channel(1);
        let thread = thread::Builder::new()
            .name("storage-ordinary-read".into())
            .spawn(move || {
                conn.run(|conn| {
                    mysql_actor::publish_result(&sender, conn.query(sql))
                        .context("publish ordinary Storage read result")?;
                    Ok(())
                })
            })?;
        Ok(Self {
            result,
            thread,
            observation: None,
            connection_id,
        })
    }
    pub fn terminal_snapshot(
        &self,
        context: &mut ScenarioContext,
        original_sql_failure: &mysql::Error,
    ) -> Result<novarocks_cluster_harness::QueryLifecycleStructuredSnapshot> {
        let snapshot = self
            .observation
            .as_ref()
            .context("ordinary Storage read has no statement observation")
            .and_then(|observation| {
                let deadline = context.deadline();
                context
                    .handle()
                    .await_query_lifecycle_statement_observation(observation, false, deadline)
            });
        with_original_sql_failure(original_sql_failure, snapshot)
    }

    pub fn finish_after<T>(
        self,
        context: &mut ScenarioContext,
        control: &mut mysql::Conn,
        phase: Result<T>,
    ) -> Result<T> {
        let interrupt = if phase.is_err() && !self.thread.is_finished() {
            control
                .query_drop(format!("KILL QUERY {}", self.connection_id))
                .context("cancel owned Storage actor during error cleanup")
        } else {
            Ok(())
        };
        let joined = self
            .thread
            .join()
            .map_err(|_| anyhow::anyhow!("Storage query actor panicked"))
            .and_then(|result| result);
        let result_error = match self.result.try_recv() {
            Ok(Err(error)) => Err(anyhow::Error::from(error)
                .context("original Storage SQL result failed during cleanup")),
            _ => Ok(()),
        };
        let drained = match self.observation {
            Some(observation) => context
                .handle()
                .drain_query_lifecycle_statement_observation(&observation),
            None => Ok(()),
        };
        with_cleanup(
            with_cleanup(
                with_cleanup(with_cleanup(phase, interrupt), joined),
                result_error,
            ),
            drained,
        )
    }
}

pub(super) struct HeldRead {
    pub read: ConnectorRead,
    pub connection_id: u32,
    observation: QueryLifecycleStatementObservation,
}
impl HeldRead {
    pub fn start(
        context: &mut ScenarioContext,
        inputs: &Inputs,
        user: &str,
        port: u16,
        token: &str,
        sql: &str,
    ) -> Result<Self> {
        exact_sql(inputs, token, sql)?;
        let conn = mysql_actor::connect_for_cancellation_until(user, port, context.deadline())?;
        let connection_id = conn.connection_id();
        let deadline = context.deadline();
        let observation = context.handle().arm_selected_statement_observation(
            token,
            connection_id,
            sql,
            deadline,
        )?;
        let (ready_tx, ready) = mpsc::sync_channel(1);
        let (done_tx, done) = mpsc::sync_channel(1);
        let (probe, probe_rx) = mpsc::sync_channel(1);
        let (probe_result_tx, probe_result) = mpsc::sync_channel(1);
        let (release, release_rx) = mpsc::channel();
        let sql = sql.to_owned();
        let worker = thread::Builder::new()
            .name("storage-exact-held-read".into())
            .spawn(move || {
                conn.run(|conn| {
                    ready_tx
                        .send(connection_id)
                        .context("publish exact held reader connection")?;
                    mysql_actor::publish_result(&done_tx, conn.query::<i64, _>(sql))
                        .context("publish exact held reader result")?;
                    probe_rx
                        .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                        .context("receive reader connection probe")?;
                    mysql_actor::publish_result(
                        &probe_result_tx,
                        conn.query_first::<i64, _>("SELECT 1"),
                    )
                    .context("publish reader connection probe")?;
                    release_rx
                        .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                        .context("release exact held reader session")?;
                    Ok(())
                })
            });
        match worker {
            Ok(thread) => Ok(Self {
                read: ConnectorRead {
                    ready,
                    done,
                    probe,
                    probe_result,
                    release,
                    thread,
                },
                connection_id,
                observation,
            }),
            Err(error) => {
                let drained = context
                    .handle()
                    .drain_query_lifecycle_statement_observation(&observation);
                with_cleanup(Err(error.into()), drained)
            }
        }
    }
    pub fn finish_after(
        self,
        context: &mut ScenarioContext,
        control: &mut mysql::Conn,
        phase: Result<()>,
    ) -> Result<()> {
        let interrupt = if phase.is_err() && !self.read.thread.is_finished() {
            let result = control
                .query_drop(format!("KILL QUERY {}", self.connection_id))
                .context("cancel owned held Storage reader during error cleanup");
            // A disconnected receiver means the actor exited; the actual join
            // below resolves that fact and preserves its error or panic.
            let _ = self.read.probe.try_send(());
            let _ = self.read.release.send(());
            result
        } else {
            Ok(())
        };
        let joined = self
            .read
            .thread
            .join()
            .map_err(|_| anyhow::anyhow!("held Storage reader panicked"))
            .and_then(|result| result);
        let result_error = match self.read.done.try_recv() {
            Ok(Err(error)) => Err(anyhow::Error::from(error)
                .context("original held Storage SQL result failed during cleanup")),
            _ => Ok(()),
        };
        let probe_error = pending_probe_error(&self.read.probe_result);
        let drained = context
            .handle()
            .drain_query_lifecycle_statement_observation(&self.observation);
        with_cleanup(
            with_cleanup(
                with_cleanup(
                    with_cleanup(with_cleanup(phase, interrupt), joined),
                    result_error,
                ),
                probe_error,
            ),
            drained,
        )
    }
}

fn pending_probe_error(receiver: &mpsc::Receiver<mysql::Result<Option<i64>>>) -> Result<()> {
    match receiver.try_recv() {
        Ok(Err(error)) => Err(anyhow::Error::from(error)
            .context("original held Storage connection probe failed during cleanup")),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_phase_preserves_unconsumed_probe_error_after_actual_join() {
        let (sender, result) = mpsc::sync_channel::<mysql::Result<Option<i64>>>(1);
        let worker = thread::spawn(move || {
            mysql_actor::publish_result(
                &sender,
                Err(mysql::Error::from(std::io::Error::other(
                    "actual probe failure",
                ))),
            )
        });
        let joined = worker.join().unwrap();
        let phase: Result<()> = Err(anyhow::anyhow!("original Storage phase failure"));
        let failure =
            with_cleanup(with_cleanup(phase, joined), pending_probe_error(&result)).unwrap_err();
        let text = format!("{failure:#}");
        assert!(text.contains("original Storage phase failure"));
        assert!(text.contains("actual probe failure"));
        // A caller-consumed probe must not produce a second cleanup failure.
        pending_probe_error(&result).unwrap();
    }

    #[test]
    fn observation_and_drain_failures_preserve_the_original_sql_failure() {
        let result: mysql::Result<Vec<i64>> =
            Err(std::io::Error::other("original SQL failure").into());
        let error = finish_observed_outcome(
            result,
            Err(anyhow::anyhow!("exact observation failure")),
            Err(anyhow::anyhow!("original drain failure")),
        )
        .err()
        .expect("all three failures must remain visible");
        let text = format!("{error:#}");
        assert!(text.contains("original SQL failure"));
        assert!(text.contains("exact observation failure"));
        assert!(text.contains("original drain failure"));
    }

    #[test]
    fn consumed_sql_failure_survives_observation_or_assertion_and_actor_cleanup_errors() {
        for failure in [
            "exact terminal observation failure",
            "exact fact/site assertion failure",
        ] {
            let (sender, receiver) = mpsc::sync_channel(1);
            let actor = thread::spawn(move || -> Result<()> {
                let sql_result: mysql::Result<Vec<i64>> =
                    Err(std::io::Error::other("consumed original SQL failure").into());
                sender
                    .send(sql_result)
                    .map_err(|_| anyhow::anyhow!("publish original SQL result"))?;
                Err(anyhow::anyhow!("original actor exit failure"))
            });
            let received = receiver.recv();
            // Observe the real actor's exit before an expectation can panic.
            let joined = actor
                .join()
                .map_err(|_| anyhow::anyhow!("original actor panicked"))
                .and_then(|result| result);
            let sql_error = received
                .expect("receive original SQL result")
                .expect_err("the original SQL failed");
            let phase: Result<()> =
                with_original_sql_failure(&sql_error, Err(anyhow::anyhow!(failure)));
            let error = with_cleanup(
                with_cleanup(phase, joined),
                Err(anyhow::anyhow!("original observation drain failure")),
            )
            .unwrap_err();
            let text = format!("{error:#}");
            assert!(text.contains("consumed original SQL failure"));
            assert!(text.contains(failure));
            assert!(text.contains("original actor exit failure"));
            assert!(text.contains("original observation drain failure"));
        }
    }

    #[test]
    fn expected_sql_failure_does_not_replace_a_successful_observation() {
        let sql_error: mysql::Error = std::io::Error::other("expected SQL failure").into();
        assert_eq!(with_original_sql_failure(&sql_error, Ok(7)).unwrap(), 7);
    }

    #[test]
    fn acquisition_mysql_error_matches_the_exact_original_safe_projection() {
        let detail = "Dependency failure (reason 23:3) at operator 7 during Pull";
        let error = mysql::Error::MySqlError(mysql::MySqlError {
            code: 1105,
            state: "HY000".into(),
            message: "[query.execution.dependency] Dependency failure (reason 23:3) at operator 7 during Pull".into(),
        });
        let server = check_acquisition_mysql_error(&error, detail).unwrap();
        assert_eq!(server.code, 1105);
        assert_eq!(server.state, "HY000");
    }

    #[test]
    fn acquisition_mysql_error_rejects_client_io_and_other_server_projections() {
        let detail = "Dependency failure (reason 23:3) at operator 7 during Pull";
        let io: mysql::Error = std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "client disconnected before receiving the server error",
        )
        .into();
        let rejected =
            with_original_sql_failure(&io, check_acquisition_mysql_error(&io, detail)).unwrap_err();
        let text = format!("{rejected:#}");
        assert!(text.contains("original MySQL server error"));
        assert!(text.contains("client disconnected before receiving the server error"));
        for (code, state, message) in [
            (
                1105,
                "HY000",
                "[query.execution.dependency] Dependency failure (reason 23:3) at operator 8 during Pull",
            ),
            (
                1045,
                "HY000",
                "[query.execution.dependency] Dependency failure (reason 23:3) at operator 7 during Pull",
            ),
            (
                1105,
                "28000",
                "[query.execution.dependency] Dependency failure (reason 23:3) at operator 7 during Pull",
            ),
        ] {
            let error = mysql::Error::MySqlError(mysql::MySqlError {
                code,
                state: state.into(),
                message: message.into(),
            });
            assert!(check_acquisition_mysql_error(&error, detail).is_err());
        }
    }

    fn authority(close: bool) -> AuthorityIntent {
        AuthorityIntent {
            catalog: "cad2_vended_catalog".into(),
            scope: "s3://warehouse/exact/".into(),
            endpoint: "http://127.0.0.1:30001/_fixture/vended-credentials/refresh".into(),
            close,
        }
    }

    #[test]
    fn observation_does_not_add_close_and_late_close_has_independent_grant() {
        let root = Path::new("/tmp/exact-storage-session");
        let observed = authority_grants(&authority(false), root);
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].parameters["backend_index"], 0);
        assert_eq!(observed[0].parameters["action"], "observe");
        let closed = authority_grants(&authority(true), root);
        assert_eq!(closed.len(), 2);
        assert_eq!(closed[1].parameters["action"], "close");
        assert_ne!(closed[0].grant_id, closed[1].grant_id);
        assert_eq!(
            closed[0].parameters["target"],
            closed[1].parameters["target"]
        );
    }

    #[test]
    fn storage_target_contains_every_exact_owner_field_and_only_be_zero() {
        let target = authority(false);
        let grants = authority_grants(&target, Path::new("/tmp/exact-storage-session"));
        let parameters = &grants[0].parameters;
        assert_eq!(parameters.as_object().unwrap().len(), 4);
        assert_eq!(parameters["backend_index"], 0);
        let frozen = &parameters["target"];
        assert_eq!(frozen.as_object().unwrap().len(), 6);
        assert_eq!(frozen["catalog"], target.catalog);
        assert_eq!(frozen["scope"], target.scope);
        assert_eq!(frozen["endpoint"], target.endpoint);
        assert_eq!(frozen["principal_name"], VENDED_EXECUTION_CREDENTIAL_NAME);
        assert_eq!(
            frozen["principal_generation"],
            VENDED_EXECUTION_CREDENTIAL_GENERATION
        );
        assert_eq!(frozen["capability"], "credentials-endpoint");
    }

    #[test]
    fn full_sql_and_run_token_must_both_match_before_statement_arm() {
        let inputs = Inputs {
            root: PathBuf::from("/tmp/exact-storage-session"),
            intent: authority(false),
            queries: BTreeMap::from([(
                "selected".into(),
                "SELECT count(*) FROM c.d.t WHERE v % 100 = 0".into(),
            )]),
        };
        assert!(
            exact_sql(
                &inputs,
                "selected",
                "SELECT count(*) FROM c.d.t WHERE v % 100 = 0"
            )
            .is_ok()
        );
        assert!(
            exact_sql(
                &inputs,
                "other",
                "SELECT count(*) FROM c.d.t WHERE v % 100 = 0"
            )
            .is_err()
        );
        assert!(
            exact_sql(
                &inputs,
                "selected",
                "SELECT count(*) FROM c.d.t WHERE v % 100 = 1"
            )
            .is_err()
        );
    }

    #[test]
    fn storage_only_plan_keeps_unrelated_roles_ordinary() {
        let directory = tempfile::tempdir().unwrap();
        let binary = directory.path().join("binary");
        fs::write(&binary, []).unwrap();
        let (plan, _) = plan(
            3,
            &binary,
            Instant::now() + Duration::from_secs(5),
            authority(false),
            &[],
        )
        .unwrap();
        let summaries = plan.planned_launch_summaries();
        let roles = summaries.as_array().unwrap();
        assert_eq!(roles.len(), 4);
        assert!(roles.iter().all(|role| role["binary"] == json!(binary)));
        for role in roles.iter().filter(|role| role["ordinary"] == true) {
            assert!(role["grants"].as_array().unwrap().is_empty());
        }
        let observed = roles
            .iter()
            .filter(|role| role["ordinary"] == false)
            .collect::<Vec<_>>();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0]["grants"].as_array().unwrap().len(), 1);
        assert_eq!(observed[0]["grants"][0]["domain"], "storage-authority");
        assert_eq!(observed[0]["grants"][0]["action"], "observe");
    }

    #[test]
    fn held_query_declares_reader_and_lease_actions_for_three_finite_roles() {
        let directory = tempfile::tempdir().unwrap();
        let binary = directory.path().join("binary");
        fs::write(&binary, []).unwrap();
        let sql = held_sql("c", "d", "t");
        let deadline = Instant::now() + Duration::from_secs(5);
        let (plan, inputs) = plan(
            3,
            &binary,
            deadline,
            authority(false),
            &[QueryIntent {
                run_token: REFRESH_HELD_TOKEN,
                sql: sql.clone(),
                lease_markers: true,
            }],
        )
        .unwrap();
        assert_eq!(plan.deadline(), deadline);
        assert_eq!(inputs.queries.len(), 1);
        assert_eq!(inputs.queries[REFRESH_HELD_TOKEN], sql);
        let summaries = plan.planned_launch_summaries();
        let roles = summaries.as_array().unwrap();
        let count = |action: &str| {
            roles
                .iter()
                .flat_map(|role| role["grants"].as_array().unwrap())
                .filter(|grant| grant["action"] == action)
                .count()
        };
        assert_eq!(count("reader-marker"), 3);
        assert_eq!(count("grpc-fragment"), 3);
        assert_eq!(count("bind-reader-marker"), 3);
        assert_eq!(count("bind-grpc-fragment"), 3);
        assert_eq!(count("close"), 0);
    }

    #[test]
    fn duplicate_query_tokens_reject_prelaunch_instead_of_redirecting_target() {
        let directory = tempfile::tempdir().unwrap();
        let binary = directory.path().join("binary");
        fs::write(&binary, []).unwrap();
        let result = plan(
            3,
            &binary,
            Instant::now() + Duration::from_secs(5),
            authority(false),
            &[
                QueryIntent {
                    run_token: FAULT_READ_TOKEN,
                    sql: "SELECT 1".into(),
                    lease_markers: false,
                },
                QueryIntent {
                    run_token: FAULT_READ_TOKEN,
                    sql: "SELECT 2".into(),
                    lease_markers: false,
                },
            ],
        );
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("finite and unique")
        );
    }

    #[test]
    fn endpoint_intent_comes_from_live_prelaunch_reservation() {
        let reserved = UnreachableEndpoint::prepare().unwrap();
        assert_ne!(reserved.address.port(), 0);
        assert_eq!(reserved.listener.local_addr().unwrap(), reserved.address);
        assert_eq!(
            reserved.endpoint,
            format!(
                "http://{}/_fixture/vended-credentials/refresh",
                reserved.address
            )
        );
    }

    #[test]
    fn primary_failure_and_cleanup_failure_are_both_preserved() {
        let failed: Result<()> = with_cleanup(
            Err(anyhow::anyhow!("actual SQL failed")),
            Err(anyhow::anyhow!("drain failed")),
        );
        let message = failed.unwrap_err().to_string();
        assert!(message.contains("actual SQL failed") && message.contains("drain failed"));
        assert!(with_cleanup(Ok(10), Err(anyhow::anyhow!("drain failed"))).is_err());
    }
}
