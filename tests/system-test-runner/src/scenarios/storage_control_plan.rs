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
    ConnectorRead, ConnectorSqlOutcome, VENDED_EXECUTION_CREDENTIAL_GENERATION,
    VENDED_EXECUTION_CREDENTIAL_NAME, connector_bigint_rows, connector_integer_rows,
    connector_sql_outcome, publish_connector_result, run_original_connector_pending,
};
use crate::actors::sql::{SqlSession, StatementFailure};
use crate::scenario::ScenarioContext;
use anyhow::{Context, Result, bail, ensure};
use novarocks_cluster_harness::test_sessions::{ClusterTestSessionPlan, RoleSlot};
use novarocks_cluster_harness::{QueryLifecycleStatementObservation, TestClusterOwner};
use novarocks_failpoint::session::DomainGrant;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::fs;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
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
    pub task_protocol_markers: bool,
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
        const READER_ACTIONS: &[&str] =
            &[novarocks_failpoint::observation_marker::ObservationAction::ReaderMarker.as_str()];
        const LEASE_ACTIONS: &[&str] = &[
            novarocks_failpoint::observation_marker::ObservationAction::ReaderMarker.as_str(),
            novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol.as_str(),
        ];
        for action in if query.task_protocol_markers {
            LEASE_ACTIONS
        } else {
            READER_ACTIONS
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
    crate::scenarios::paimon::scan_statement_actor::combine(primary, cleanup)
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
fn arm_original_pending(
    context: &mut ScenarioContext,
    token: &str,
    sql: &str,
    pending: novarocks_test_trino_client::PendingStatement,
) -> Result<(
    novarocks_test_trino_client::PendingStatement,
    QueryLifecycleStatementObservation,
)> {
    let cancel = pending.cancel_handle();
    let deadline = context.deadline();
    match context
        .handle()
        .arm_selected_statement_observation_for_client_query(
            token,
            &pending.query_id,
            sql,
            deadline,
        ) {
        Ok(observation) => Ok((pending, observation)),
        Err(primary) => with_cleanup(
            Err(primary),
            cancel.delete(deadline).map_err(anyhow::Error::from),
        ),
    }
}
pub(super) fn outcome(
    context: &mut ScenarioContext,
    inputs: &Inputs,
    conn: &mut SqlSession,
    token: &str,
    sql: &str,
) -> Result<std::result::Result<Vec<i64>, StatementFailure>> {
    exact_sql(inputs, token, sql)?;
    let pending = conn.submit(sql)?;
    let (pending, observation) = arm_original_pending(context, token, sql, pending)?;
    let result = run_original_connector_pending(conn, pending, context.deadline());
    let drained = {
        let original_deadline = context.deadline();
        context
            .handle()
            .drain_query_lifecycle_statement_binding(&observation, original_deadline)
    };
    match result {
        Ok(result) => match (result, drained) {
            (result, Ok(())) => Ok(result.and_then(connector_bigint_rows)),
            (Ok(_), Err(cleanup)) => Err(cleanup),
            (Err(primary), Err(cleanup)) => Err(anyhow::Error::new(primary)
                .context(format!("Storage observation drain failed: {cleanup:#}"))),
        },
        Err(primary) => with_cleanup(Err(primary), drained),
    }
}
/// Original typed SQL response and the same observation's actual terminal.
pub(super) struct ObservedStorageOutcome {
    pub result: std::result::Result<Vec<i64>, StatementFailure>,
    pub snapshot: novarocks_cluster_harness::QueryLifecycleStructuredSnapshot,
}
fn with_original_sql_failure<T>(original: &StatementFailure, outcome: Result<T>) -> Result<T> {
    match outcome {
        Ok(value) => Ok(value),
        Err(observed) => Err(anyhow::Error::new(original.clone()).context(format!(
            "Storage observation/assertion failed: {observed:#}"
        ))),
    }
}
fn finish_observed_outcome(
    result: std::result::Result<Vec<i64>, StatementFailure>,
    snapshot: Result<novarocks_cluster_harness::QueryLifecycleStructuredSnapshot>,
    drained: Result<()>,
) -> Result<ObservedStorageOutcome> {
    let snapshot = with_cleanup(snapshot, drained);
    let snapshot = match &result {
        Ok(_) => snapshot.context("original Storage SQL succeeded"),
        Err(error) => with_original_sql_failure(error, snapshot),
    }?;
    Ok(ObservedStorageOutcome { result, snapshot })
}
pub(super) fn observed_outcome(
    context: &mut ScenarioContext,
    inputs: &Inputs,
    conn: &mut SqlSession,
    token: &str,
    sql: &str,
) -> Result<ObservedStorageOutcome> {
    exact_sql(inputs, token, sql)?;
    let pending = conn.submit(sql)?;
    let (pending, observation) = arm_original_pending(context, token, sql, pending)?;
    let result = run_original_connector_pending(conn, pending, context.deadline());
    let deadline = context.deadline();
    let snapshot = context
        .handle()
        .await_query_lifecycle_statement_observation(&observation, false, deadline);
    let drained = context
        .handle()
        .drain_query_lifecycle_statement_binding(&observation, deadline);
    match result {
        Ok(result) => {
            finish_observed_outcome(result.and_then(connector_bigint_rows), snapshot, drained)
        }
        Err(primary) => with_cleanup(Err(primary), with_cleanup(snapshot.map(|_| ()), drained)),
    }
}

pub(super) fn assert_acquisition_failure(
    context: &mut ScenarioContext,
    snapshot: &novarocks_cluster_harness::QueryLifecycleStructuredSnapshot,
    code: u16,
    original_sql_failure: &StatementFailure,
) -> Result<()> {
    let asserted = (|| {
        check_acquisition_failure(context, snapshot, code)?;
        let detail = snapshot
            .failure_detail
            .as_deref()
            .context("Storage acquisition failure omitted its original safe detail")?;
        let server = check_acquisition_sql_error(original_sql_failure, detail)?;
        context.action(format!(
            "observed original acquisition SQL error code={} name={} type={} descriptor=query.execution.dependency",
            server.error_code, server.error_name, server.error_type
        ));
        Ok(())
    })();
    with_original_sql_failure(original_sql_failure, asserted)
}

/// Cross-check the original server packet against the same statement's
/// owner-authored safe detail. A client transport failure proves no server
/// failure descriptor, even if an independent Native terminal was observed.
fn check_acquisition_sql_error<'a>(
    error: &'a StatementFailure,
    safe_detail: &str,
) -> Result<&'a novarocks_test_trino_client::TrinoError> {
    let StatementFailure::Trino(server) = error else {
        bail!("Storage acquisition failure did not return an original Trino server error: {error}");
    };
    ensure!(
        server.error_code == 65542
            && server.error_name == "REMOTE_TASK_ERROR"
            && server.error_type == "INTERNAL_ERROR",
        "Storage acquisition failure changed its exact SQL error identity: {server:?}"
    );
    let expected = format!("[query.execution.dependency] {safe_detail}");
    ensure!(
        server.message == expected,
        "Storage acquisition SQL message differs from its original safe projection: expected={expected:?} observed={:?}",
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
        snapshot.worker_participations_converged,
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

pub(super) fn query(
    context: &mut ScenarioContext,
    inputs: &Inputs,
    conn: &mut SqlSession,
    token: &str,
    sql: &str,
) -> Result<Vec<i64>> {
    outcome(context, inputs, conn, token, sql)?.map_err(anyhow::Error::from)
}

pub(super) struct SimpleRead {
    pub result: mpsc::Receiver<std::result::Result<Vec<i64>, StatementFailure>>,
    thread: thread::JoinHandle<Result<()>>,
    observation: Option<QueryLifecycleStatementObservation>,
    cancel: novarocks_test_trino_client::CancelHandle,
    deadline: Instant,
    outcome: Arc<Mutex<Option<ConnectorSqlOutcome>>>,
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
        let conn = SqlSession::new(user, port, context.deadline())?;
        let pending = conn.submit(sql)?;
        let (pending, observation) = arm_original_pending(context, token, sql, pending)?;
        Self::spawn(
            context,
            conn,
            pending,
            Some(observation),
            "storage-exact-read",
        )
    }
    pub fn start_ordinary(
        context: &ScenarioContext,
        user: &str,
        port: u16,
        sql: &str,
    ) -> Result<Self> {
        let mut conn = SqlSession::new(user, port, context.deadline())?;
        let pending = conn.submit(sql)?;
        let cancel = pending.cancel_handle();
        let deadline = context.deadline();
        let outcome = Arc::new(Mutex::new(None));
        let saved = outcome.clone();
        let (sender, result) = mpsc::sync_channel(1);
        let worker = thread::Builder::new()
            .name("storage-ordinary-read".into())
            .spawn(move || {
                let result = conn.run_pending(pending);
                *saved
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                    Some(connector_sql_outcome(&result));
                publish_connector_result(&sender, result.and_then(connector_bigint_rows))
                    .context("publish ordinary Storage read result")
            });
        match worker {
            Ok(thread) => Ok(Self {
                result,
                thread,
                observation: None,
                cancel,
                deadline,
                outcome,
            }),
            Err(primary) => with_cleanup(
                Err(primary.into()),
                cancel.delete(deadline).map_err(anyhow::Error::from),
            ),
        }
    }
    fn spawn(
        context: &mut ScenarioContext,
        mut conn: SqlSession,
        pending: novarocks_test_trino_client::PendingStatement,
        observation: Option<QueryLifecycleStatementObservation>,
        name: &'static str,
    ) -> Result<Self> {
        let cancel = pending.cancel_handle();
        let deadline = context.deadline();
        let outcome = Arc::new(Mutex::new(None));
        let saved = outcome.clone();
        let (sender, result) = mpsc::sync_channel(1);
        let worker = thread::Builder::new().name(name.into()).spawn(move || {
            let result = conn.run_pending(pending);
            *saved
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                Some(connector_sql_outcome(&result));
            publish_connector_result(&sender, result.and_then(connector_bigint_rows))
                .context("publish exact Storage read result")
        });
        match worker {
            Ok(thread) => Ok(Self {
                result,
                thread,
                observation,
                cancel,
                deadline,
                outcome,
            }),
            Err(primary) => {
                let cancelled = cancel.delete(deadline).map_err(anyhow::Error::from);
                let drained = observation.map_or(Ok(()), |observation| {
                    context
                        .handle()
                        .drain_query_lifecycle_statement_binding(&observation, deadline)
                });
                with_cleanup(Err(primary.into()), with_cleanup(cancelled, drained))
            }
        }
    }
    pub fn terminal_snapshot(
        &self,
        context: &mut ScenarioContext,
        original_sql_failure: &StatementFailure,
    ) -> Result<novarocks_cluster_harness::QueryLifecycleStructuredSnapshot> {
        let deadline = context.deadline();
        let snapshot = self
            .observation
            .as_ref()
            .context("ordinary Storage read has no statement observation")
            .and_then(|observation| {
                context
                    .handle()
                    .await_query_lifecycle_statement_observation(observation, false, deadline)
            });
        with_original_sql_failure(original_sql_failure, snapshot)
    }
    pub fn finish_after<T>(
        self,
        context: &mut ScenarioContext,
        _control: &mut SqlSession,
        phase: Result<T>,
    ) -> Result<T> {
        let before = *self
            .outcome
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let attempted = phase.is_err()
            && !matches!(
                before,
                Some(ConnectorSqlOutcome::Success | ConnectorSqlOutcome::PublicFailure)
            );
        let mut cancelled = if attempted {
            self.cancel
                .delete(self.deadline)
                .map_err(anyhow::Error::from)
        } else {
            Ok(())
        };
        if cancelled.is_err() {
            cancelled = with_cleanup(cancelled, context.shutdown());
        }
        let joined = self
            .thread
            .join()
            .map_err(|_| anyhow::anyhow!("Storage query actor panicked"))
            .and_then(|result| result);
        let after = *self
            .outcome
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !attempted
            && !matches!(
                after,
                Some(ConnectorSqlOutcome::Success | ConnectorSqlOutcome::PublicFailure)
            )
        {
            let deleted = self
                .cancel
                .delete(self.deadline)
                .map_err(anyhow::Error::from);
            cancelled = with_cleanup(
                cancelled,
                if deleted.is_err() {
                    with_cleanup(deleted, context.shutdown())
                } else {
                    deleted
                },
            );
        }
        let result_error = match self.result.try_recv() {
            Ok(Err(primary)) => Err(anyhow::Error::new(primary)
                .context("original Storage SQL result failed during cleanup")),
            _ => Ok(()),
        };
        let drained = self.observation.map_or(Ok(()), |observation| {
            context
                .handle()
                .drain_query_lifecycle_statement_binding(&observation, self.deadline)
        });
        with_cleanup(
            with_cleanup(
                with_cleanup(with_cleanup(phase, cancelled), joined),
                result_error,
            ),
            drained,
        )
    }
}

pub(super) struct HeldRead {
    pub read: ConnectorRead,
    pub client_query_id: String,
    pub cancel: novarocks_test_trino_client::CancelHandle,
    deadline: Instant,
    observation: QueryLifecycleStatementObservation,
    outcome: Arc<Mutex<Option<ConnectorSqlOutcome>>>,
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
        let deadline = context.deadline();
        let mut conn = SqlSession::new(user, port, deadline)?;
        let pending = conn.submit(sql)?;
        let client_query_id = pending.query_id.clone();
        let cancel = pending.cancel_handle();
        let (pending, observation) = arm_original_pending(context, token, sql, pending)?;
        let (ready_tx, ready) = mpsc::sync_channel(1);
        let (done_tx, done) = mpsc::sync_channel(1);
        let (probe, probe_rx) = mpsc::sync_channel(1);
        let (probe_result_tx, probe_result) = mpsc::sync_channel(1);
        let (release, release_rx) = mpsc::channel();
        let outcome = Arc::new(Mutex::new(None));
        let saved = outcome.clone();
        let id = client_query_id.clone();
        let worker = thread::Builder::new()
            .name("storage-exact-held-read".into())
            .spawn(move || {
                ready_tx
                    .send(id)
                    .context("publish exact held actual client query")?;
                let result = conn.run_pending(pending);
                *saved
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                    Some(connector_sql_outcome(&result));
                publish_connector_result(&done_tx, result.and_then(connector_bigint_rows))
                    .context("publish exact held reader result")?;
                probe_rx
                    .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                    .context("receive reader session probe")?;
                let result = match conn.submit("SELECT 1") {
                    Ok(pending) => run_original_connector_pending(&mut conn, pending, deadline)?
                        .and_then(|result| {
                            connector_integer_rows(result).and_then(|rows| match rows.as_slice() {
                                [value] => Ok(Some(*value)),
                                _ => Err(StatementFailure::Protocol(format!(
                                    "Storage probe expected one integer row: {rows:?}"
                                ))),
                            })
                        }),
                    Err(error) => Err(error),
                };
                publish_connector_result(&probe_result_tx, result)
                    .context("publish reader session probe")?;
                release_rx
                    .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                    .context("release exact held reader session")?;
                Ok(())
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
                client_query_id,
                cancel,
                deadline,
                observation,
                outcome,
            }),
            Err(primary) => {
                let cancelled = cancel.delete(deadline).map_err(anyhow::Error::from);
                let drained = context
                    .handle()
                    .drain_query_lifecycle_statement_binding(&observation, deadline);
                with_cleanup(Err(primary.into()), with_cleanup(cancelled, drained))
            }
        }
    }
    pub fn finish_after(
        self,
        context: &mut ScenarioContext,
        _control: &mut SqlSession,
        phase: Result<()>,
    ) -> Result<()> {
        let before = *self
            .outcome
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let attempted = phase.is_err()
            && !matches!(
                before,
                Some(ConnectorSqlOutcome::Success | ConnectorSqlOutcome::PublicFailure)
            );
        let mut cancelled = if attempted {
            self.cancel
                .delete(self.deadline)
                .map_err(anyhow::Error::from)
        } else {
            Ok(())
        };
        if cancelled.is_err() {
            cancelled = with_cleanup(cancelled, context.shutdown());
        }
        if phase.is_err() {
            let _ = self.read.probe.try_send(());
            let _ = self.read.release.send(());
        }
        let joined = self
            .read
            .thread
            .join()
            .map_err(|_| anyhow::anyhow!("held Storage reader panicked"))
            .and_then(|result| result);
        let after = *self
            .outcome
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !attempted
            && !matches!(
                after,
                Some(ConnectorSqlOutcome::Success | ConnectorSqlOutcome::PublicFailure)
            )
        {
            let deleted = self
                .cancel
                .delete(self.deadline)
                .map_err(anyhow::Error::from);
            cancelled = with_cleanup(
                cancelled,
                if deleted.is_err() {
                    with_cleanup(deleted, context.shutdown())
                } else {
                    deleted
                },
            );
        }
        let result_error = match self.read.done.try_recv() {
            Ok(Err(primary)) => Err(anyhow::Error::new(primary)
                .context("original held Storage SQL result failed during cleanup")),
            _ => Ok(()),
        };
        let probe_error = pending_probe_error(&self.read.probe_result);
        let drained = context
            .handle()
            .drain_query_lifecycle_statement_binding(&self.observation, self.deadline);
        with_cleanup(
            with_cleanup(
                with_cleanup(
                    with_cleanup(with_cleanup(phase, cancelled), joined),
                    result_error,
                ),
                probe_error,
            ),
            drained,
        )
    }
}

fn pending_probe_error(
    receiver: &mpsc::Receiver<std::result::Result<Option<i64>, StatementFailure>>,
) -> Result<()> {
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
        let (sender, result) =
            mpsc::sync_channel::<std::result::Result<Option<i64>, StatementFailure>>(1);
        let worker = thread::spawn(move || {
            publish_connector_result(
                &sender,
                Err(StatementFailure::Transport("actual probe failure".into())),
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
        let result: std::result::Result<Vec<i64>, StatementFailure> =
            Err(StatementFailure::Transport("original SQL failure".into()));
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
                let sql_result: std::result::Result<Vec<i64>, StatementFailure> = Err(
                    StatementFailure::Transport("consumed original SQL failure".into()),
                );
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
        let sql_error = StatementFailure::Transport("expected SQL failure".into());
        assert_eq!(with_original_sql_failure(&sql_error, Ok(7)).unwrap(), 7);
    }

    #[test]
    fn acquisition_sql_error_matches_the_exact_original_safe_projection() {
        let detail = "Dependency failure (reason 23:3) at operator 7 during Pull";
        let error = StatementFailure::Trino(serde_json::from_value(json!({
            "errorCode":65542, "errorName":"REMOTE_TASK_ERROR", "errorType":"INTERNAL_ERROR",
            "message":"[query.execution.dependency] Dependency failure (reason 23:3) at operator 7 during Pull",
        })).unwrap());
        let server = check_acquisition_sql_error(&error, detail).unwrap();
        assert_eq!(server.error_code, 65542);
        assert_eq!(server.error_name, "REMOTE_TASK_ERROR");
        assert_eq!(server.error_type, "INTERNAL_ERROR");
    }
    #[test]
    fn acquisition_sql_error_rejects_client_io_and_other_server_projections() {
        let detail = "Dependency failure (reason 23:3) at operator 7 during Pull";
        let io = StatementFailure::Transport(
            "client disconnected before receiving the server error".into(),
        );
        let rejected =
            with_original_sql_failure(&io, check_acquisition_sql_error(&io, detail)).unwrap_err();
        let text = format!("{rejected:#}");
        assert!(text.contains("original Trino server error"));
        assert!(text.contains("client disconnected before receiving the server error"));
        for (code, error_type, message) in [
            (
                65542,
                "INTERNAL_ERROR",
                "[query.execution.dependency] Dependency failure (reason 23:3) at operator 8 during Pull",
            ),
            (
                46,
                "INTERNAL_ERROR",
                "[query.execution.dependency] Dependency failure (reason 23:3) at operator 7 during Pull",
            ),
            (
                65542,
                "USER_ERROR",
                "[query.execution.dependency] Dependency failure (reason 23:3) at operator 7 during Pull",
            ),
        ] {
            let error = StatementFailure::Trino(serde_json::from_value(json!({"errorCode":code,"errorName":"REMOTE_TASK_ERROR","errorType":error_type,"message":message})).unwrap());
            assert!(check_acquisition_sql_error(&error, detail).is_err());
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
                task_protocol_markers: true,
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
        assert_eq!(
            count(
                novarocks_failpoint::observation_marker::ObservationAction::ReaderMarker.as_str()
            ),
            3
        );
        assert_eq!(
            count(
                novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol.as_str()
            ),
            3
        );
        assert_eq!(count("bind-reader-marker"), 3);
        assert_eq!(count("bind-task-protocol"), 3);
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
                    task_protocol_markers: false,
                },
                QueryIntent {
                    run_token: FAULT_READ_TOKEN,
                    sql: "SELECT 2".into(),
                    task_protocol_markers: false,
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
