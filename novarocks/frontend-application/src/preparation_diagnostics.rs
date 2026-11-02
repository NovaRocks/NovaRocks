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

//! Authenticated, bounded statement observations for functional diagnostics.
//!
//! One explicit role-session grant owns bounded observation state.
//! Observation never controls product execution.

use novarocks_failpoint::scope::{ActionClaim, ControlCancellation, GrantHandle, ProcessBinding};
use novarocks_failpoint::target_binding::{StatementInputNomination, StatementWorkNomination};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::query_execution::lifecycle_diagnostics::QueryLifecycleConvergenceSnapshot;
use novarocks_query_application::session_control::{SessionToken, StatementToken};
use novarocks_types::{QueryExecutionId, QueryId};

#[derive(Debug, Deserialize)]
pub(crate) struct ControlRequest {
    pub run_token: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StatementObservationArmRequest {
    pub run_token: String,
    pub connection_id: u32,
    pub sql_sha256: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct StatementObservationResponse {
    pub schema_version: u8,
    pub run_token: String,
    pub work_id: Option<String>,
    pub logical_execution_id: Option<String>,
    pub execution_id: Option<String>,
    pub query_process_namespace: Option<String>,
    pub query_local_sequence: Option<u64>,
    pub attempt_id: Option<u64>,
    pub snapshot: Option<serde_json::Value>,
}

const MAX_STATEMENT_OBSERVERS: usize = 64;

pub(crate) type StatementInputFreezer =
    Arc<dyn Fn(&StatementInputNomination) -> Result<(), String> + Send + Sync>;
pub(crate) type StatementNominationFreezer = Arc<
    dyn Fn(&StatementInputNomination, &StatementWorkNomination) -> Result<(), String> + Send + Sync,
>;
pub(crate) type StatementSessionResolver =
    Arc<dyn Fn(u32) -> Result<SessionToken, String> + Send + Sync>;

/// An observation capability only: neither its lifetime nor its contents
/// control admission, execution, recovery, result visibility, or cleanup.
#[derive(Clone, Debug)]
pub(crate) struct StatementObservationHandle {
    run_token: String,
    generation: u64,
    owner: Option<Weak<StatementObservationOwner>>,
}

struct StatementObserver {
    generation: u64,
    session: SessionToken,
    connection_id: u32,
    sql_digest: [u8; 32],
    statement: Option<StatementToken>,
    logical_query: Option<QueryId>,
    execution: Option<QueryExecutionId>,
    snapshot: Option<Box<QueryLifecycleConvergenceSnapshot>>,
}

#[derive(Default)]
struct StatementObservers {
    next_generation: u64,
    runs: BTreeMap<String, StatementObserver>,
}

impl StatementObservers {
    fn validate_arm(&self, request: &StatementObservationArmRequest) -> Result<[u8; 32], String> {
        if request.connection_id == 0 {
            return Err("statement observation connection id must be nonzero".into());
        }
        if request.run_token.trim().is_empty() || request.run_token.len() > 256 {
            return Err("statement observation token must contain 1..256 bytes".into());
        }
        if self.runs.contains_key(&request.run_token)
            || self
                .runs
                .values()
                .any(|run| run.connection_id == request.connection_id)
        {
            return Err("statement observation token or connection is already armed".into());
        }
        if self.runs.len() >= MAX_STATEMENT_OBSERVERS {
            return Err("statement observation capacity exhausted".into());
        }
        if request.sql_sha256.len() != 64
            || !request
                .sql_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err("statement SQL digest must be lowercase SHA-256 hex".into());
        }
        self.next_generation
            .checked_add(1)
            .ok_or("statement observation generation exhausted")?;
        let mut sql_digest = [0u8; 32];
        for (index, byte) in sql_digest.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&request.sql_sha256[index * 2..index * 2 + 2], 16)
                .expect("validated SHA-256 hex");
        }
        Ok(sql_digest)
    }
    fn arm(
        &mut self,
        request: StatementObservationArmRequest,
        session: SessionToken,
    ) -> Result<(), String> {
        let sql_digest = self.validate_arm(&request)?;
        let generation = self
            .next_generation
            .checked_add(1)
            .expect("arm validated the unchanged journal generation");
        self.next_generation = generation;
        self.runs.insert(
            request.run_token,
            StatementObserver {
                generation,
                session,
                connection_id: request.connection_id,
                sql_digest,
                statement: None,
                logical_query: None,
                execution: None,
                snapshot: None,
            },
        );
        Ok(())
    }

    fn bind_statement(
        &mut self,
        statement: StatementToken,
        sql: &str,
    ) -> Option<StatementObservationHandle> {
        if statement.generation() == 0 {
            return None;
        }
        let digest: [u8; 32] = Sha256::digest(sql.as_bytes()).into();
        let (token, run) = self
            .runs
            .iter_mut()
            .find(|(_, run)| run.session == statement.session() && run.sql_digest == digest)?;
        if run.statement.is_some_and(|bound| bound != statement) {
            return None;
        }
        run.statement = Some(statement);
        Some(StatementObservationHandle {
            run_token: token.clone(),
            generation: run.generation,
            owner: None,
        })
    }

    fn bound_statement(&self, statement: StatementToken) -> Option<StatementObservationHandle> {
        self.runs
            .iter()
            .find(|(_, run)| run.statement == Some(statement))
            .map(|(token, run)| StatementObservationHandle {
                run_token: token.clone(),
                generation: run.generation,
                owner: None,
            })
    }

    fn run_mut(&mut self, handle: &StatementObservationHandle) -> Option<&mut StatementObserver> {
        self.runs
            .get_mut(&handle.run_token)
            .filter(|run| run.generation == handle.generation)
    }

    fn bind_attempt(&mut self, handle: &StatementObservationHandle, execution: QueryExecutionId) {
        let Some(run) = self.run_mut(handle) else {
            return;
        };
        if run.statement.is_none()
            || run
                .logical_query
                .is_some_and(|query| query != execution.query_id())
        {
            return;
        }
        if run
            .execution
            .is_some_and(|bound| bound.attempt_id().get() >= execution.attempt_id().get())
        {
            return;
        }
        run.logical_query = Some(execution.query_id());
        run.execution = Some(execution);
        run.snapshot = None;
    }

    fn publish(&mut self, snapshot: &QueryLifecycleConvergenceSnapshot) {
        for run in self.runs.values_mut() {
            if run.execution == Some(snapshot.execution_id) && run.snapshot.is_none() {
                run.snapshot = Some(Box::new(snapshot.clone()));
            }
        }
    }

    fn response(&self, token: &str) -> Result<StatementObservationResponse, String> {
        let run = self
            .runs
            .get(token)
            .ok_or_else(|| "statement observation token is not armed".to_owned())?;
        let attribution =
            run.execution
                .map(|execution| {
                    execution.query_id().process_attribution().ok_or_else(|| {
                        "observed execution has no exact process attribution".to_owned()
                    })
                })
                .transpose()?;
        let snapshot = run.snapshot.as_deref().map(|snapshot| {
            debug_assert_eq!(run.execution, Some(snapshot.execution_id));
            crate::native::report_server::lifecycle_convergence_debug_json(snapshot.clone())
        });
        Ok(StatementObservationResponse {
            schema_version: 1,
            run_token: token.to_owned(),
            work_id: run.statement.map(|statement| {
                format!(
                    "statement:{}:{}:{}",
                    run.connection_id,
                    statement.session().key().get(),
                    statement.generation()
                )
            }),
            logical_execution_id: run
                .logical_query
                .map(|query| format!("{}:{}", query.high(), query.low())),
            execution_id: run.execution.map(|execution| {
                format!(
                    "{}:{}:{}",
                    execution.query_id().high(),
                    execution.query_id().low(),
                    execution.attempt_id().get()
                )
            }),
            query_process_namespace: attribution
                .map(|attribution| attribution.namespace().to_string()),
            query_local_sequence: attribution.map(|attribution| attribution.sequence().get()),
            attempt_id: run.execution.map(|execution| execution.attempt_id().get()),
            snapshot,
        })
    }

    fn drain(&mut self, token: &str) -> Result<StatementObservationResponse, String> {
        let response = self.response(token)?;
        self.runs.remove(token);
        Ok(response)
    }
}

/// The role composition passes this owner explicitly to HTTP and statement callers.
/// A closed session invalidates every old handle and releases the bounded journal.
pub(crate) struct StatementObservationOwner {
    authorization: DiagnosticAuthorization,
    cancellation: ControlCancellation,
    claim: Mutex<ActionClaim>,
    observed_hit: std::sync::atomic::AtomicBool,
    input_freezer: std::sync::OnceLock<StatementInputFreezer>,
    nomination_freezer: std::sync::OnceLock<StatementNominationFreezer>,
    session_resolver: std::sync::OnceLock<StatementSessionResolver>,
    state: Mutex<StatementObservers>,
}
impl std::fmt::Debug for StatementObservationOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatementObservationOwner")
            .finish_non_exhaustive()
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ObservationGrant {
    action: String,
    bearer_secret: String,
}
impl StatementObservationOwner {
    pub(crate) const DOMAIN: &'static str = "statement-observation";
    pub(crate) fn from_grants(grants: Vec<GrantHandle>) -> Result<Option<Arc<Self>>, String> {
        if grants.is_empty() {
            return Ok(None);
        }
        if grants.iter().any(|grant| grant.domain() != Self::DOMAIN) {
            return Err("statement observation received a foreign domain grant".into());
        }
        if grants.len() != 1 {
            return Err("statement observation requires exactly one grant".into());
        }
        let grant = grants.into_iter().next().expect("one grant");
        let parameters: ObservationGrant = serde_json::from_value(grant.parameters().clone())
            .map_err(|_| "statement observation grant is invalid".to_owned())?;
        if parameters.action != "observe" {
            return Err("statement observation action is unsupported".into());
        }
        let authorization = DiagnosticAuthorization::from_secret(Some(&parameters.bearer_secret));
        if !authorization.enabled() {
            return Err("statement observation secret is invalid".into());
        }
        let cancellation = grant.cancellation();
        if !matches!(cancellation.process(), ProcessBinding::Frontend(_)) {
            return Err("statement observation requires the actual FE process".into());
        }
        let claim = grant
            .claim_once(cancellation.process())
            .map_err(|_| "statement observation claim refused".to_owned())?;
        Ok(Some(Arc::new(Self {
            authorization,
            cancellation,
            claim: Mutex::new(claim),
            observed_hit: std::sync::atomic::AtomicBool::new(false),
            input_freezer: std::sync::OnceLock::new(),
            nomination_freezer: std::sync::OnceLock::new(),
            session_resolver: std::sync::OnceLock::new(),
            state: Mutex::new(StatementObservers::default()),
        })))
    }
    pub(crate) fn is_active(&self) -> bool {
        self.cancellation.is_active()
    }
    fn active(&self, state: &mut StatementObservers) -> bool {
        if self.cancellation.is_active() {
            true
        } else {
            state.runs.clear();
            false
        }
    }
    pub(crate) fn authorize(&self, header: Option<&str>) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        self.active(&mut state) && self.authorization.authorized(header)
    }
    pub(crate) fn arm(&self, request: StatementObservationArmRequest) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "statement observation lock poisoned")?;
        if !self.active(&mut state) {
            return Err("statement observation session is closed".into());
        }
        // Reject malformed, duplicate, full or exhausted journals before a
        // finite domain can preserve an otherwise unaccepted input.
        let sql_digest = state.validate_arm(&request)?;
        let session = self
            .session_resolver
            .get()
            .ok_or("statement observation session owner is not installed")?(
            request.connection_id
        )?;
        let input = self.input_for_session(
            session,
            request.connection_id,
            &request.run_token,
            sql_digest,
        )?;
        self.input_freezer
            .get()
            .ok_or("statement input freezer is not installed")?(&input)?;
        state.arm(request, session)
    }
    fn input_for_session(
        &self,
        session: SessionToken,
        connection_id: u32,
        run_token: &str,
        sql_sha256: [u8; 32],
    ) -> Result<StatementInputNomination, String> {
        let ProcessBinding::Frontend(process) = self.cancellation.process() else {
            return Err("statement input requires the actual frontend process".into());
        };
        let input = StatementInputNomination {
            target_session_id: self.cancellation.session_id().into(),
            target_launch_id: self.cancellation.launch_id().into(),
            statement_run_token: run_token.into(),
            frontend_process_id: process.to_string(),
            connection_id,
            session_key: session.key().get(),
            sql_sha256,
        };
        input.validate().map_err(|error| error.to_string())?;
        Ok(input)
    }
    pub(crate) fn peek(&self, token: &str) -> Result<StatementObservationResponse, String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "statement observation lock poisoned".to_owned())?;
        if !self.active(&mut state) {
            return Err("statement observation session is closed".into());
        }
        state.response(token)
    }
    pub(crate) fn drain(&self, token: &str) -> Result<StatementObservationResponse, String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "statement observation lock poisoned".to_owned())?;
        if !self.active(&mut state) {
            return Err("statement observation session is closed".into());
        }
        state.drain(token)
    }
    /// Server composition installs the one actual QueryControl owner. Neither
    /// the request nor the declaration can supply this identity resolver.
    pub(crate) fn install_session_resolver(
        &self,
        resolver: StatementSessionResolver,
    ) -> Result<(), String> {
        self.session_resolver
            .set(resolver)
            .map_err(|_| "statement observation session owner was already installed".to_owned())
    }
    pub(crate) fn install_input_freezer(
        &self,
        freezer: StatementInputFreezer,
    ) -> Result<(), String> {
        self.input_freezer
            .set(freezer)
            .map_err(|_| "statement input freezer was already installed".into())
    }
    pub(crate) fn install_nomination_freezer(
        &self,
        freezer: StatementNominationFreezer,
    ) -> Result<(), String> {
        self.nomination_freezer
            .set(freezer)
            .map_err(|_| "statement nomination freezer was already installed".to_owned())
    }
    pub(crate) fn bind_statement(
        self: &Arc<Self>,
        statement: StatementToken,
        sql: &str,
    ) -> Result<Option<StatementObservationHandle>, String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "statement observation lock poisoned".to_owned())?;
        if !self.active(&mut state) {
            return Ok(None);
        }
        let Some(mut handle) = state.bind_statement(statement, sql) else {
            return Ok(None);
        };
        let ProcessBinding::Frontend(process) = self.cancellation.process() else {
            return Err("statement nomination requires the actual frontend process".into());
        };
        let nomination = novarocks_failpoint::target_binding::StatementWorkNomination {
            target_session_id: self.cancellation.session_id().to_owned(),
            statement_run_token: handle.run_token.clone(),
            frontend_process_id: process.to_string(),
            connection_id: state.runs[&handle.run_token].connection_id,
            session_key: statement.session().key().get(),
            statement_generation: statement.generation(),
        };
        nomination.validate().map_err(|error| error.to_string())?;
        let input = self.input_for_session(
            statement.session(),
            state.runs[&handle.run_token].connection_id,
            &handle.run_token,
            Sha256::digest(sql.as_bytes()).into(),
        )?;
        input
            .validate_statement(&nomination)
            .map_err(|error| error.to_string())?;
        // Pure owner-local pins run under the same journal lock as bind, so
        // drain/rearm cannot redirect an unclaimed grant to a later generation.
        if let Some(freezer) = self.nomination_freezer.get() {
            freezer(&input, &nomination)?;
        }
        if !self
            .observed_hit
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            self.claim
                .lock()
                .map_err(|_| "statement observation claim lock poisoned".to_owned())?
                .record_hit()
                .map_err(|error| error.to_string())?;
        }
        handle.owner = Some(Arc::downgrade(self));
        Ok(Some(handle))
    }
    /// Borrow only the existing journal entry named by the complete nomination.
    /// The role's grant and active journal remain the observation authority.
    pub(crate) fn observation_for_nomination(
        self: &Arc<Self>,
        nomination: &StatementWorkNomination,
    ) -> Option<StatementObservationHandle> {
        nomination.validate().ok()?;
        let handle = {
            let mut state = self.state.lock().ok()?;
            if !self.active(&mut state) {
                return None;
            }
            let run = state.runs.get(&nomination.statement_run_token)?;
            run.statement?;
            StatementObservationHandle {
                run_token: nomination.statement_run_token.clone(),
                generation: run.generation,
                owner: Some(Arc::downgrade(self)),
            }
        };
        // Observation loss never refuses the original product job. A drain
        // between these locks invalidates this exact journal generation.
        match handle.nomination() {
            Some(expected) if &expected == nomination => Some(handle),
            _ => None,
        }
    }

    pub(crate) fn bound_statement(
        self: &Arc<Self>,
        statement: StatementToken,
    ) -> Option<StatementObservationHandle> {
        let mut state = self.state.lock().ok()?;
        if !self.active(&mut state) {
            return None;
        }
        let mut handle = state.bound_statement(statement)?;
        handle.owner = Some(Arc::downgrade(self));
        Some(handle)
    }
    pub(crate) fn publish(&self, snapshot: &QueryLifecycleConvergenceSnapshot) {
        if let Ok(mut state) = self.state.lock()
            && self.active(&mut state)
        {
            state.publish(snapshot);
        }
    }
    pub(crate) fn close_observation(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.runs.clear();
        }
    }
}
impl StatementObservationHandle {
    pub(crate) fn nomination(
        &self,
    ) -> Option<novarocks_failpoint::target_binding::StatementWorkNomination> {
        self.nomination_for_observed_execution(None)
    }

    /// Refine statement evidence through the existing journal's exact binding.
    /// Other logical work and superseded attempts keep executing unobserved.
    pub(crate) fn nomination_for_exact_execution(
        &self,
        execution: QueryExecutionId,
    ) -> Option<novarocks_failpoint::target_binding::StatementWorkNomination> {
        self.nomination_for_observed_execution(Some(execution))
    }

    fn nomination_for_observed_execution(
        &self,
        execution: Option<QueryExecutionId>,
    ) -> Option<novarocks_failpoint::target_binding::StatementWorkNomination> {
        let owner = self.owner.as_ref()?.upgrade()?;
        let mut state = owner.state.lock().ok()?;
        if !owner.active(&mut state) {
            return None;
        }
        let run = state.run_mut(self)?;
        if execution.is_some_and(|execution| run.execution != Some(execution)) {
            return None;
        }
        let statement = run.statement?;
        let ProcessBinding::Frontend(process) = owner.cancellation.process() else {
            return None;
        };
        Some(
            novarocks_failpoint::target_binding::StatementWorkNomination {
                target_session_id: owner.cancellation.session_id().to_owned(),
                statement_run_token: self.run_token.clone(),
                frontend_process_id: process.to_string(),
                connection_id: run.connection_id,
                session_key: statement.session().key().get(),
                statement_generation: statement.generation(),
            },
        )
    }

    /// A nomination is observation identity only, never injection authority.
    pub(crate) fn statement_target(&self) -> Option<(String, StatementToken, u32)> {
        let owner = self.owner.as_ref()?.upgrade()?;
        let mut state = owner.state.lock().ok()?;
        if !owner.active(&mut state) {
            return None;
        }
        let run = state.run_mut(self)?;
        Some((self.run_token.clone(), run.statement?, run.connection_id))
    }
    pub(crate) fn bind_attempt(&self, execution: QueryExecutionId) {
        let Some(owner) = self.owner.as_ref().and_then(Weak::upgrade) else {
            return;
        };
        if let Ok(mut state) = owner.state.lock()
            && owner.active(&mut state)
        {
            state.bind_attempt(self, execution);
        }
    }
}

/// The credential owner has no collector, observer generation or mutation.
struct DiagnosticAuthorization {
    secret_digest: Option<[u8; 32]>,
}

impl DiagnosticAuthorization {
    fn from_secret(secret: Option<&str>) -> Self {
        Self {
            secret_digest: secret
                .filter(|value| value.len() >= 32)
                .map(|value| Sha256::digest(value.as_bytes()).into()),
        }
    }

    fn enabled(&self) -> bool {
        self.secret_digest.is_some()
    }

    fn authorized(&self, header: Option<&str>) -> bool {
        let Some(secret_digest) = self.secret_digest else {
            return false;
        };
        header
            .and_then(|value| value.strip_prefix("Bearer "))
            .map(|candidate| <[u8; 32]>::from(Sha256::digest(candidate.as_bytes())))
            .is_some_and(|candidate_digest| candidate_digest == secret_digest)
    }
}

#[cfg(test)]
mod authorization_tests {
    use super::*;

    #[test]
    fn unconfigured_and_short_secrets_disable_statement_authorization() {
        for secret in [
            None,
            Some(""),
            Some("short"),
            Some("1234567890123456789012345678901"),
        ] {
            let auth = DiagnosticAuthorization::from_secret(secret);
            assert!(!auth.enabled());
            assert!(!auth.authorized(None));
            assert!(!auth.authorized(Some("Bearer anything")));
        }
        // Preserve the existing byte-length policy, not a Unicode character count.
        assert!(DiagnosticAuthorization::from_secret(Some(&"é".repeat(16))).enabled());
    }

    #[test]
    fn exact_bearer_digest_authorizes_the_unchanged_statement_observer() {
        let secret = "s".repeat(32);
        let auth = DiagnosticAuthorization::from_secret(Some(&secret));
        assert!(auth.enabled());
        assert_eq!(
            auth.secret_digest,
            Some(Sha256::digest(secret.as_bytes()).into())
        );
        for header in [
            None,
            Some("Bearer wrong"),
            Some(secret.as_str()),
            Some("bearer ssssssssssssssssssssssssssssssss"),
            Some("Bearer\tssssssssssssssssssssssssssssssss"),
            Some("Bearer ssssssssssssssssssssssssssssssss "),
        ] {
            assert!(!auth.authorized(header));
        }
        assert!(auth.authorized(Some(&format!("Bearer {secret}"))));
        let mut observers = StatementObservers::default();
        observers
            .arm(
                StatementObservationArmRequest {
                    run_token: "authorized-statement".to_owned(),
                    connection_id: 91,
                    sql_sha256: format!("{:x}", Sha256::digest(b"SELECT 1")),
                },
                SessionToken::new(
                    novarocks_query_application::session_control::ClientSessionKey::new(
                        (91_u64 << 32) | 1_u64,
                    )
                    .expect("nonzero fixture session"),
                ),
            )
            .expect("the authorized statement owner remains available");
        assert_eq!(
            observers
                .response("authorized-statement")
                .unwrap()
                .run_token,
            "authorized-statement"
        );
        observers.drain("authorized-statement").unwrap();
        assert!(observers.response("authorized-statement").is_err());
    }
}

#[cfg(test)]
mod statement_observation_tests {
    use super::*;
    use crate::query_execution::lifecycle_diagnostics::{
        RuntimeFilterTerminalRollupSnapshot, RuntimeFilterTerminalRollupUnavailable,
    };
    use novarocks_types::AttemptId;

    fn arm_request(token: &str, connection_id: u32, sql: &str) -> StatementObservationArmRequest {
        StatementObservationArmRequest {
            run_token: token.to_owned(),
            connection_id,
            sql_sha256: format!("{:x}", Sha256::digest(sql.as_bytes())),
        }
    }

    fn statement(connection: u32, epoch: u64, generation: u64) -> StatementToken {
        StatementToken::new(
            SessionToken::new(
                novarocks_query_application::session_control::ClientSessionKey::new(
                    (u64::from(connection) << 32) | epoch,
                )
                .unwrap(),
            ),
            generation,
        )
    }

    fn execution(high: i64, low: i64, attempt: u64) -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(high, low), AttemptId::new(attempt).unwrap()).unwrap()
    }

    fn snapshot(execution_id: QueryExecutionId) -> QueryLifecycleConvergenceSnapshot {
        QueryLifecycleConvergenceSnapshot {
            execution_id,
            primary_error: None,
            failure: None,
            worker_contexts_converged: false,
            runtime_filter: RuntimeFilterTerminalRollupSnapshot::Unavailable(
                RuntimeFilterTerminalRollupUnavailable::TerminalOutcomesIncomplete,
            ),
        }
    }

    #[test]
    fn statement_observation_matches_exact_connection_sql_and_statement_owner() {
        let mut state = StatementObservers::default();
        let session_key = (7_u64 << 32) | 9;
        state
            .arm(
                arm_request("selected", 7, "SELECT k FROM t"),
                SessionToken::new(
                    novarocks_query_application::session_control::ClientSessionKey::new(
                        session_key,
                    )
                    .expect("nonzero fixture session"),
                ),
            )
            .unwrap();
        assert!(
            state
                .bind_statement(statement(8, 1, 1), "SELECT k FROM t")
                .is_none()
        );
        assert!(
            state
                .bind_statement(statement(7, 1, 1), "SELECT k FROM other")
                .is_none()
        );
        assert!(state.response("selected").unwrap().work_id.is_none());
        assert!(
            state
                .bind_statement(statement(7, 0, 12), "SELECT k FROM t")
                .is_none()
        );
        assert!(
            state
                .bind_statement(statement(7, 9, 0), "SELECT k FROM t")
                .is_none()
        );
        let token = statement(7, 9, 12);
        let handle = state.bind_statement(token, "SELECT k FROM t").unwrap();
        assert!(
            state
                .bind_statement(statement(7, 9, 13), "SELECT k FROM t")
                .is_none()
        );
        assert!(
            state
                .bind_statement(statement(7, 10, 12), "SELECT k FROM t")
                .is_none()
        );
        assert!(state.bound_statement(statement(7, 10, 12)).is_none());
        let exact = execution(-19, 41, 1);
        state.bind_attempt(&handle, exact);
        state.bind_attempt(&handle, execution(-19, 99, 2));
        let response = state.response("selected").unwrap();
        assert_eq!(
            response.work_id,
            Some(format!("statement:7:{session_key}:12"))
        );
        assert_eq!(response.logical_execution_id.as_deref(), Some("-19:41"));
        assert_eq!(response.execution_id.as_deref(), Some("-19:41:1"));
        assert_eq!(
            response.query_process_namespace.as_deref(),
            Some("0xffffffffffffffed")
        );
        assert_eq!(response.query_local_sequence, Some(41));
        assert_eq!(response.attempt_id, Some(1));
    }

    #[test]
    fn statement_observation_ignores_late_previous_query_and_old_attempt_snapshots() {
        let mut state = StatementObservers::default();
        state
            .arm(
                arm_request("selected", 7, "SELECT k FROM t"),
                SessionToken::new(
                    novarocks_query_application::session_control::ClientSessionKey::new(
                        (7_u64 << 32) | 9_u64,
                    )
                    .expect("nonzero fixture session"),
                ),
            )
            .unwrap();
        let previous = execution(-19, 40, 1);
        state.publish(&snapshot(previous));
        let handle = state
            .bind_statement(statement(7, 9, 12), "SELECT k FROM t")
            .unwrap();
        let first = execution(-19, 41, 1);
        state.bind_attempt(&handle, first);
        state.publish(&snapshot(previous));
        assert!(state.response("selected").unwrap().snapshot.is_none());
        state.publish(&snapshot(first));
        state.bind_attempt(&handle, first);
        assert!(state.response("selected").unwrap().snapshot.is_some());
        let replacement = execution(-19, 41, 2);
        state.bind_attempt(&handle, replacement);
        assert!(state.response("selected").unwrap().snapshot.is_none());
        state.bind_attempt(&handle, first);
        state.publish(&snapshot(first));
        state.publish(&snapshot(execution(19, 41, 2)));
        assert!(state.response("selected").unwrap().snapshot.is_none());
        state.publish(&snapshot(replacement));
        let response = state.response("selected").unwrap();
        assert_eq!(response.execution_id.as_deref(), Some("-19:41:2"));
        let wire = response.snapshot.unwrap();
        assert_eq!(wire["execution_id"], "-19:41:2");
        assert_eq!(
            wire["query_process_namespace"],
            response.query_process_namespace.unwrap()
        );
        assert_eq!(
            wire["query_local_sequence"],
            response.query_local_sequence.unwrap()
        );
        assert_eq!(wire["query_attempt_id"], response.attempt_id.unwrap());
    }

    #[test]
    fn session_capture_before_close_cannot_bind_the_replacement_after_arm() {
        let control = Arc::new(
            novarocks_query_application::query_control::QueryApplicationControl::default(),
        );
        let service =
            novarocks_query_application::session_control::QueryControlService::new(control.clone());
        let original = service
            .register_session(
                novarocks_query_application::session_control::SessionIdentity::new(
                    novarocks_query_application::client_connection::ClientConnectionToken::new(
                        7, 1,
                    )
                    .unwrap(),
                    "capture-race",
                ),
            )
            .unwrap();
        let captured = control.current_session_for_test_controls(7).unwrap();
        drop(original);
        let replacement = service
            .register_session(
                novarocks_query_application::session_control::SessionIdentity::new(
                    novarocks_query_application::client_connection::ClientConnectionToken::new(
                        7, 2,
                    )
                    .unwrap(),
                    "capture-race",
                ),
            )
            .unwrap();
        let mut state = StatementObservers::default();
        state
            .arm(arm_request("captured", 7, "SELECT 1"), captured)
            .unwrap();
        let mut actual = service.begin_statement(replacement.token()).unwrap();
        assert!(state.bind_statement(actual.token(), "SELECT 1").is_none());
        assert!(state.response("captured").unwrap().work_id.is_none());
        actual.finish();
        drop(replacement);
    }

    #[test]
    fn statement_observation_parallel_tokens_are_bounded_and_drain_invalidates_handles() {
        let mut state = StatementObservers::default();
        assert!(
            state
                .arm(
                    arm_request("zero", 0, "SELECT 1"),
                    SessionToken::new(
                        novarocks_query_application::session_control::ClientSessionKey::new(1_u64)
                            .expect("nonzero fixture session")
                    )
                )
                .is_err()
        );
        for connection in 1..=MAX_STATEMENT_OBSERVERS as u32 {
            state
                .arm(
                    arm_request(&format!("run-{connection}"), connection, "SELECT 1"),
                    SessionToken::new(
                        novarocks_query_application::session_control::ClientSessionKey::new(
                            (u64::from(connection) << 32) | 1,
                        )
                        .unwrap(),
                    ),
                )
                .unwrap();
        }
        assert!(
            state
                .arm(
                    arm_request("run-1", 100, "SELECT 1"),
                    SessionToken::new(
                        novarocks_query_application::session_control::ClientSessionKey::new(
                            (100_u64 << 32) | 1_u64
                        )
                        .expect("nonzero fixture session")
                    )
                )
                .is_err()
        );
        assert!(
            state
                .arm(
                    arm_request("another", 1, "SELECT 1"),
                    SessionToken::new(
                        novarocks_query_application::session_control::ClientSessionKey::new(
                            (1_u64 << 32) | 1_u64
                        )
                        .expect("nonzero fixture session")
                    )
                )
                .is_err()
        );
        assert!(
            state
                .arm(
                    arm_request("overflow", 100, "SELECT 1"),
                    SessionToken::new(
                        novarocks_query_application::session_control::ClientSessionKey::new(
                            (100_u64 << 32) | 1_u64
                        )
                        .expect("nonzero fixture session")
                    )
                )
                .is_err()
        );
        let a = state
            .bind_statement(statement(1, 1, 1), "SELECT 1")
            .unwrap();
        let b = state
            .bind_statement(statement(2, 1, 1), "SELECT 1")
            .unwrap();
        state.bind_attempt(&a, execution(5, 41, 1));
        state.bind_attempt(&b, execution(5, 42, 1));
        state.publish(&snapshot(execution(5, 42, 1)));
        assert!(state.response("run-1").unwrap().snapshot.is_none());
        assert!(state.response("run-2").unwrap().snapshot.is_some());
        state.drain("run-1").unwrap();
        assert!(state.response("run-1").is_err());
        state
            .arm(
                arm_request("run-1", 1, "SELECT 1"),
                SessionToken::new(
                    novarocks_query_application::session_control::ClientSessionKey::new(
                        (1_u64 << 32) | 1_u64,
                    )
                    .expect("nonzero fixture session"),
                ),
            )
            .unwrap();
        state.bind_attempt(&a, execution(5, 43, 2));
        state.publish(&snapshot(execution(5, 41, 1)));
        assert!(state.response("run-1").unwrap().work_id.is_none());
        assert!(state.response("run-1").unwrap().execution_id.is_none());
        assert!(state.response("run-1").unwrap().snapshot.is_none());
        assert_eq!(state.runs.len(), MAX_STATEMENT_OBSERVERS);
        state.next_generation = u64::MAX;
        state.drain("run-1").unwrap();
        assert!(
            state
                .arm(
                    arm_request("exhausted", 1, "SELECT 1"),
                    SessionToken::new(
                        novarocks_query_application::session_control::ClientSessionKey::new(
                            (1_u64 << 32) | 1_u64
                        )
                        .expect("nonzero fixture session")
                    )
                )
                .is_err()
        );
    }
}

#[cfg(all(test, unix))]
mod session_owner_tests {
    use super::*;
    use novarocks_failpoint::liveness::{PreparedRoleSession, RunnerSession};
    use novarocks_failpoint::session::{
        DECLARATION_VERSION, DomainGrant, SessionDeclaration, SessionRole,
    };
    use std::os::unix::fs::DirBuilderExt;

    #[test]
    fn explicit_observer_session_end_invalidates_generation_and_releases_journal() {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let root = std::path::PathBuf::from(format!(
            "/tmp/g2diag-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&root)
            .unwrap();
        let mut declaration = SessionDeclaration {
            schema_version: DECLARATION_VERSION,
            session_id: "diag-session".into(),
            launch_id: "diag-launch".into(),
            role: SessionRole::Fe,
            lifetime_millis: 10_000,
            liveness_socket: root.join("live.sock"),
            handshake_secret: "abcdefghijklmnopqrstuvwxyz0123456789".into(),
            domain_grants: vec![DomainGrant {
                domain: StatementObservationOwner::DOMAIN.into(),
                grant_id: "observe".into(),
                parameters: serde_json::json!({"action":"observe","bearer_secret":"abcdefghijklmnopqrstuvwxyz0123456789"}),
            }],
        };
        let mut foreign = declaration.domain_grants[0].clone();
        foreign.domain = "foreign-domain".into();
        foreign.grant_id = "foreign-grant".into();
        declaration.domain_grants.push(foreign);
        let path = root.join("declaration.json");
        declaration.write_private(&path).unwrap();
        let prepared = PreparedRoleSession::read_and_claim(&path, SessionRole::Fe).unwrap();
        let mut runner = RunnerSession::start(declaration).unwrap();
        let mut role = prepared
            .bind(ProcessBinding::Frontend(
                novarocks_types::FrontendProcessId::new_v7(),
            ))
            .unwrap();
        assert!(
            StatementObservationOwner::from_grants(role.scope().grants_for("foreign-domain"))
                .is_err()
        );
        let owner = StatementObservationOwner::from_grants(
            role.scope().grants_for(StatementObservationOwner::DOMAIN),
        )
        .unwrap()
        .unwrap();
        let control = Arc::new(
            novarocks_query_application::query_control::QueryApplicationControl::default(),
        );
        let service =
            novarocks_query_application::session_control::QueryControlService::new(control.clone());
        let session = service
            .register_session(
                novarocks_query_application::session_control::SessionIdentity::new(
                    novarocks_query_application::client_connection::ClientConnectionToken::new(
                        7, 1,
                    )
                    .unwrap(),
                    "observer-test",
                ),
            )
            .unwrap();
        owner
            .install_session_resolver(Arc::new(move |connection| {
                control
                    .current_session_for_test_controls(connection)
                    .map_err(|error| {
                        format!("statement observation session lookup failed: {error:?}")
                    })
            }))
            .unwrap();
        owner.install_input_freezer(Arc::new(|_| Ok(()))).unwrap();
        owner
            .install_nomination_freezer(Arc::new(|_, _| Ok(())))
            .unwrap();
        assert!(!owner.authorize(Some("Bearer wrong")));
        owner
            .arm(StatementObservationArmRequest {
                run_token: "one".into(),
                connection_id: 7,
                sql_sha256: format!("{:x}", Sha256::digest(b"SELECT 1")),
            })
            .unwrap();
        let mut actual = service.begin_statement(session.token()).unwrap();
        let token = actual.token();
        let handle = owner.bind_statement(token, "SELECT 1").unwrap().unwrap();
        assert_eq!(handle.statement_target(), Some(("one".into(), token, 7)));
        assert!(
            StatementObservationOwner::from_grants(
                role.scope().grants_for(StatementObservationOwner::DOMAIN)
            )
            .is_err()
        );
        let nomination = handle.nomination().unwrap();
        assert!(owner.observation_for_nomination(&nomination).is_some());
        role.close_and_join().unwrap();
        assert!(owner.observation_for_nomination(&nomination).is_none());
        assert!(handle.statement_target().is_none());
        assert!(owner.peek("one").is_err());
        assert!(!owner.authorize(Some("Bearer abcdefghijklmnopqrstuvwxyz0123456789")));
        assert!(owner.state.lock().unwrap().runs.is_empty());
        actual.finish();
        drop(session);
        runner.end_and_join().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
    struct ObserverFixture {
        root: std::path::PathBuf,
        owner: Arc<StatementObservationOwner>,
        service: novarocks_query_application::session_control::QueryControlService,
        role: novarocks_failpoint::liveness::RoleSessionOwner,
        runner: RunnerSession,
    }
    impl ObserverFixture {
        fn new() -> Self {
            Self::with_input_freezer(Arc::new(|_| Ok(())))
        }
        fn with_input_freezer(freezer: StatementInputFreezer) -> Self {
            let root = std::path::PathBuf::from("/tmp")
                .join(format!("g2observe-{}", uuid::Uuid::now_v7()));
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(&root)
                .unwrap();
            let declaration = SessionDeclaration {
                schema_version: DECLARATION_VERSION,
                session_id: "observer-only".into(),
                launch_id: "observer-only-launch".into(),
                role: SessionRole::Fe,
                lifetime_millis: 10_000,
                liveness_socket: root.join("live.sock"),
                handshake_secret: "abcdefghijklmnopqrstuvwxyz0123456789".into(),
                domain_grants: vec![DomainGrant {
                    domain: StatementObservationOwner::DOMAIN.into(),
                    grant_id: "observe".into(),
                    parameters: serde_json::json!({"action":"observe","bearer_secret":"abcdefghijklmnopqrstuvwxyz0123456789"}),
                }],
            };
            let path = root.join("declaration.json");
            declaration.write_private(&path).unwrap();
            let prepared = PreparedRoleSession::read_and_claim(&path, SessionRole::Fe).unwrap();
            let runner = RunnerSession::start(declaration).unwrap();
            let role = prepared
                .bind(ProcessBinding::Frontend(
                    novarocks_types::FrontendProcessId::new_v7(),
                ))
                .unwrap();
            let owner = StatementObservationOwner::from_grants(
                role.scope().grants_for(StatementObservationOwner::DOMAIN),
            )
            .unwrap()
            .unwrap();
            let control = Arc::new(
                novarocks_query_application::query_control::QueryApplicationControl::default(),
            );
            let service = novarocks_query_application::session_control::QueryControlService::new(
                control.clone(),
            );
            owner
                .install_session_resolver(Arc::new(move |connection| {
                    control
                        .current_session_for_test_controls(connection)
                        .map_err(|error| format!("statement session lookup failed: {error:?}"))
                }))
                .unwrap();
            owner.install_input_freezer(freezer).unwrap();
            owner
                .install_nomination_freezer(Arc::new(|_, _| Ok(())))
                .unwrap();
            Self {
                root,
                owner,
                service,
                role,
                runner,
            }
        }
        fn session(
            &self,
            connection: u32,
            generation: u64,
        ) -> novarocks_query_application::session_control::QuerySessionLease {
            self.service
                .register_session(
                    novarocks_query_application::session_control::SessionIdentity::new(
                        novarocks_query_application::client_connection::ClientConnectionToken::new(
                            connection, generation,
                        )
                        .unwrap(),
                        "observer-only",
                    ),
                )
                .unwrap()
        }
        fn arm(token: &str, connection_id: u32, sql: &str) -> StatementObservationArmRequest {
            StatementObservationArmRequest {
                run_token: token.into(),
                connection_id,
                sql_sha256: format!("{:x}", Sha256::digest(sql.as_bytes())),
            }
        }
    }
    impl Drop for ObserverFixture {
        fn drop(&mut self) {
            self.role.close_and_join().unwrap();
            self.runner.end_and_join().unwrap();
            std::fs::remove_dir_all(&self.root).unwrap();
        }
    }

    #[test]
    fn finite_statement_nomination_only_projects_its_existing_exact_query_binding() {
        let execution = |low, attempt| {
            QueryExecutionId::new(
                QueryId::new(-19, low),
                novarocks_types::AttemptId::new(attempt).unwrap(),
            )
            .unwrap()
        };
        let snapshot = |execution_id| {
            use crate::query_execution::lifecycle_diagnostics::{
                RuntimeFilterTerminalRollupSnapshot, RuntimeFilterTerminalRollupUnavailable,
            };
            QueryLifecycleConvergenceSnapshot {
                execution_id,
                primary_error: None,
                failure: None,
                worker_contexts_converged: false,
                runtime_filter: RuntimeFilterTerminalRollupSnapshot::Unavailable(
                    RuntimeFilterTerminalRollupUnavailable::TerminalOutcomesIncomplete,
                ),
            }
        };
        let fixture = ObserverFixture::new();
        let session = fixture.session(7, 1);
        fixture
            .owner
            .arm(ObserverFixture::arm("cohorts", 7, "CALL rewrite()"))
            .unwrap();
        let mut statement = fixture.service.begin_statement(session.token()).unwrap();
        let statement_token = statement.token();
        assert!(fixture.owner.bound_statement(statement_token).is_none());
        let handle = fixture
            .owner
            .bind_statement(statement.token(), "CALL rewrite()")
            .unwrap()
            .unwrap();
        let nomination = handle.nomination().unwrap();
        // The command consumer only borrows the already bound original handle.
        let command_handle = fixture.owner.bound_statement(statement_token).unwrap();
        assert_eq!(command_handle.nomination(), Some(nomination.clone()));
        // Explicitly submitted background work may bind after SQL returns.
        // Only the finite observation owner and exact journal grant evidence.
        statement.finish();
        let first = execution(41, 1);
        let other_cohort = execution(42, 1);
        let replacement = execution(41, 2);
        assert!(handle.nomination_for_exact_execution(first).is_none());
        command_handle.bind_attempt(first);
        assert_eq!(
            handle.nomination_for_exact_execution(first),
            Some(nomination.clone())
        );
        fixture.owner.publish(&snapshot(first));
        handle.bind_attempt(first);
        assert_eq!(
            handle.nomination_for_exact_execution(first),
            Some(nomination.clone())
        );
        assert!(fixture.owner.peek("cohorts").unwrap().snapshot.is_some());
        handle.bind_attempt(other_cohort);
        assert!(
            handle
                .nomination_for_exact_execution(other_cohort)
                .is_none()
        );
        assert!(
            handle
                .nomination_for_exact_execution(other_cohort)
                .is_none()
        );
        assert_eq!(
            handle.nomination_for_exact_execution(first),
            Some(nomination.clone())
        );
        assert_eq!(
            fixture
                .owner
                .peek("cohorts")
                .unwrap()
                .execution_id
                .as_deref(),
            Some("-19:41:1")
        );
        handle.bind_attempt(replacement);
        assert_eq!(
            handle.nomination_for_exact_execution(replacement),
            Some(nomination.clone())
        );
        assert!(fixture.owner.peek("cohorts").unwrap().snapshot.is_none());
        handle.bind_attempt(first);
        assert!(handle.nomination_for_exact_execution(first).is_none());
        assert!(handle.nomination_for_exact_execution(first).is_none());
        assert_eq!(
            handle.nomination_for_exact_execution(replacement),
            Some(nomination.clone())
        );
        fixture.owner.publish(&snapshot(replacement));
        handle.bind_attempt(replacement);
        assert_eq!(
            handle.nomination_for_exact_execution(replacement),
            Some(nomination.clone())
        );
        assert!(fixture.owner.peek("cohorts").unwrap().snapshot.is_some());
        // Exact execution refinement leaves the parent nomination available.
        assert_eq!(handle.nomination(), Some(nomination));
        fixture.owner.drain("cohorts").unwrap();
        assert!(fixture.owner.bound_statement(statement_token).is_none());
        assert!(
            command_handle
                .nomination_for_exact_execution(replacement)
                .is_none()
        );
        assert!(handle.nomination_for_exact_execution(replacement).is_none());
    }

    #[test]
    fn nominated_observer_requires_the_same_complete_active_journal_entry() {
        let fixture = ObserverFixture::new();
        let session = fixture.session(7, 1);
        fixture
            .owner
            .arm(ObserverFixture::arm("stats", 7, "ANALYZE TABLE t"))
            .unwrap();
        let mut statement = fixture.service.begin_statement(session.token()).unwrap();
        assert!(
            fixture
                .owner
                .observation_for_nomination(&StatementWorkNomination {
                    target_session_id: fixture.owner.cancellation.session_id().to_owned(),
                    statement_run_token: "stats".into(),
                    frontend_process_id: match fixture.owner.cancellation.process() {
                        ProcessBinding::Frontend(process) => process.to_string(),
                        _ => unreachable!(),
                    },
                    connection_id: 7,
                    session_key: statement.token().session().key().get(),
                    statement_generation: statement.token().generation(),
                })
                .is_none(),
            "an armed but unbound run grants no observation"
        );
        let original = fixture
            .owner
            .bind_statement(statement.token(), "ANALYZE TABLE t")
            .unwrap()
            .unwrap();
        let nomination = original.nomination().unwrap();
        statement.finish();
        let resolved = fixture
            .owner
            .observation_for_nomination(&nomination)
            .unwrap();
        assert_eq!(resolved.generation, original.generation);
        assert!(Weak::ptr_eq(
            resolved.owner.as_ref().unwrap(),
            original.owner.as_ref().unwrap()
        ));
        for field in 0..5 {
            let mut foreign = nomination.clone();
            match field {
                0 => foreign.target_session_id.push_str("-foreign"),
                1 => {
                    foreign.frontend_process_id =
                        novarocks_types::FrontendProcessId::new_v7().to_string()
                }
                2 => foreign.connection_id += 1,
                3 => foreign.session_key += 1,
                4 => foreign.statement_generation += 1,
                _ => unreachable!(),
            }
            assert!(
                fixture.owner.observation_for_nomination(&foreign).is_none(),
                "foreign field {field}"
            );
        }
        let mut missing = nomination.clone();
        missing.statement_run_token = "other-run".into();
        assert!(fixture.owner.observation_for_nomination(&missing).is_none());
        fixture.owner.drain("stats").unwrap();
        assert!(
            fixture
                .owner
                .observation_for_nomination(&nomination)
                .is_none()
        );
        assert!(resolved.nomination().is_none());
        fixture
            .owner
            .arm(ObserverFixture::arm("stats", 7, "ANALYZE TABLE t"))
            .unwrap();
        let mut replacement = fixture.service.begin_statement(session.token()).unwrap();
        let next = fixture
            .owner
            .bind_statement(replacement.token(), "ANALYZE TABLE t")
            .unwrap()
            .unwrap();
        assert!(
            fixture
                .owner
                .observation_for_nomination(&nomination)
                .is_none()
        );
        assert!(
            fixture
                .owner
                .observation_for_nomination(&next.nomination().unwrap())
                .is_some()
        );
        assert!(
            resolved.nomination().is_none(),
            "rearming cannot revive the old journal generation"
        );
        replacement.finish();
    }

    #[tokio::test]
    async fn submitted_statistics_originals_resolve_the_existing_observer_after_sql_returns() {
        use novarocks_statistics_application::{
            StatisticsAttemptError, StatisticsAttemptExecutor, StatisticsColumns, StatisticsJob,
            StatisticsJobAdmission, StatisticsJobCreate, StatisticsJobProgress,
            StatisticsJobRepository, StatisticsPublicationOutcome, StatisticsTarget,
            StatisticsWorker,
        };
        struct ObserveAttempt {
            owner: Weak<StatementObservationOwner>,
            seen: Mutex<Option<StatementObservationHandle>>,
        }
        impl StatisticsAttemptExecutor for ObserveAttempt {
            fn execute<'a>(
                &'a self,
                job: StatisticsJob,
                admission: StatisticsJobAdmission,
                progress: StatisticsJobProgress,
            ) -> std::pin::Pin<
                Box<
                    dyn std::future::Future<Output = Result<(), StatisticsAttemptError>>
                        + Send
                        + 'a,
                >,
            > {
                Box::pin(async move {
                    let mut originals = admission.into_originals();
                    originals.owner.as_ref().unwrap().scope().check().unwrap();
                    let nomination = originals.test_statement_nomination.take().unwrap();
                    let handle = self
                        .owner
                        .upgrade()
                        .and_then(|owner| owner.observation_for_nomination(&nomination));
                    *self.seen.lock().unwrap() = handle;
                    progress.prepared(job.updated_at_ms).unwrap();
                    progress.collected(job.updated_at_ms).unwrap();
                    // This observation fixture starts no physical computation.
                    progress
                        .retire_unmounted(originals, job.updated_at_ms)
                        .unwrap();
                    progress
                        .finish(
                            Ok(StatisticsPublicationOutcome::NoOp {
                                finalization_failure: None,
                                cleanup_failure: None,
                            }),
                            job.updated_at_ms,
                        )
                        .unwrap();
                    Ok(())
                })
            }
        }
        for rearm_before_worker in [false, true] {
            let fixture = ObserverFixture::new();
            let session = fixture.session(7, 1);
            fixture
                .owner
                .arm(ObserverFixture::arm("stats-job", 7, "ANALYZE TABLE t"))
                .unwrap();
            let mut statement = fixture.service.begin_statement(session.token()).unwrap();
            let original = fixture
                .owner
                .bind_statement(statement.token(), "ANALYZE TABLE t")
                .unwrap()
                .unwrap();
            let nomination = Arc::new(original.nomination().unwrap());
            let control = novarocks_workload_control::WorkloadControl::try_new(
                novarocks_workload_control::WorkloadConfig::default(),
                novarocks_workload_control::ResourceConfig {
                    total_bytes: 16 * 1024 * 1024,
                    control_bytes: 1024 * 1024,
                    per_scope_bytes: 8 * 1024 * 1024,
                },
            )
            .unwrap();
            control
                .configure_result_capacity(novarocks_workload_control::ResultCapacityConfig::V1)
                .unwrap();
            control.mark_ready().unwrap();
            let root = control
                .root_admission()
                .begin_warehouse_root(novarocks_workload_control::WorkRequest::new(
                    novarocks_workload_control::WorkClass::Statistics,
                ))
                .unwrap();
            let (permit, window) = root
                .owner
                .scope()
                .admit_query_with_result(novarocks_workload_control::ResultWindowClass::Internal)
                .unwrap()
                .await
                .unwrap();
            let admission = StatisticsJobAdmission::try_new(root.owner, permit, window)
                .unwrap()
                .with_test_statement_nomination(nomination)
                .unwrap();
            let repository = StatisticsJobRepository::new();
            let job = repository
                .create_admitted(
                    StatisticsJobCreate {
                        target: StatisticsTarget {
                            catalog: Arc::from("iceberg"),
                            namespace: Arc::from("db"),
                            table: Arc::from("t"),
                            object_id: Arc::from(&b"original-object"[..]),
                        },
                        columns: StatisticsColumns::All,
                        submitted_at_ms: 1,
                    },
                    admission,
                )
                .await
                .unwrap();
            statement.finish();
            let replacement = if rearm_before_worker {
                fixture.owner.drain("stats-job").unwrap();
                fixture
                    .owner
                    .arm(ObserverFixture::arm(
                        "stats-job",
                        7,
                        "ANALYZE TABLE replacement",
                    ))
                    .unwrap();
                let mut next = fixture.service.begin_statement(session.token()).unwrap();
                let replacement = fixture
                    .owner
                    .bind_statement(next.token(), "ANALYZE TABLE replacement")
                    .unwrap()
                    .unwrap();
                next.finish();
                Some(replacement)
            } else {
                None
            };
            let executor = Arc::new(ObserveAttempt {
                owner: Arc::downgrade(&fixture.owner),
                seen: Mutex::new(None),
            });
            let worker = StatisticsWorker::new(repository, executor.clone());
            let terminal = worker.run_one(2).await.unwrap().unwrap();
            assert_eq!(terminal.id, job.id);
            assert!(terminal.state.is_terminal());
            assert_eq!(
                terminal.state,
                novarocks_statistics_application::StatisticsJobState::Terminal(
                    novarocks_statistics_application::StatisticsJobConclusion::Succeeded,
                ),
            );
            let handle = executor.seen.lock().unwrap().take();
            if let Some(replacement) = replacement {
                assert!(handle.is_none(), "the old job has no successor observation");
                assert!(original.nomination().is_none());
                assert!(replacement.nomination().is_some());
                let response = fixture.owner.peek("stats-job").unwrap();
                assert!(response.execution_id.is_none());
                assert!(response.logical_execution_id.is_none());
                assert!(response.snapshot.is_none());
                fixture.owner.drain("stats-job").unwrap();
                assert!(replacement.nomination().is_none());
            } else {
                let handle = handle.unwrap();
                assert_eq!(handle.nomination(), original.nomination());
                let execution = QueryExecutionId::new(
                    QueryId::new(-19, 41),
                    novarocks_types::AttemptId::new(1).unwrap(),
                )
                .unwrap();
                handle.bind_attempt(execution);
                assert_eq!(
                    handle.nomination_for_exact_execution(execution),
                    original.nomination()
                );
                fixture.owner.drain("stats-job").unwrap();
                assert!(handle.nomination_for_exact_execution(execution).is_none());
            }
            assert_eq!(control.snapshot().root_responsibilities, 0);
            control.close_admission();
            assert!(control.shutdown().is_ok());
        }
    }

    #[test]
    fn observer_only_same_token_can_drain_and_reuse_with_actual_replacement_session() {
        let fixture = ObserverFixture::new();
        let original = fixture.session(7, 1);
        fixture
            .owner
            .arm(ObserverFixture::arm("reused", 7, "SELECT 1"))
            .unwrap();
        assert!(
            !fixture
                .role
                .scope()
                .observations()
                .iter()
                .any(|event| event.event == "hit")
        );
        let original_session = original.token();
        let mut first = fixture.service.begin_statement(original_session).unwrap();
        let old_handle = fixture
            .owner
            .bind_statement(first.token(), "SELECT 1")
            .unwrap()
            .unwrap();
        first.finish();
        fixture.owner.drain("reused").unwrap();
        assert!(old_handle.statement_target().is_none());
        drop(original);
        let replacement = fixture.session(7, 2);
        fixture
            .owner
            .arm(ObserverFixture::arm("reused", 7, "SELECT 2"))
            .unwrap();
        let mut next = fixture
            .service
            .begin_statement(replacement.token())
            .unwrap();
        let new_handle = fixture
            .owner
            .bind_statement(next.token(), "SELECT 2")
            .unwrap()
            .unwrap();
        assert_eq!(
            new_handle.statement_target(),
            Some(("reused".into(), next.token(), 7))
        );
        assert_ne!(next.token().session(), original_session);
        next.finish();
        fixture.owner.drain("reused").unwrap();
        assert_eq!(
            fixture
                .role
                .scope()
                .observations()
                .iter()
                .filter(|event| event.event == "hit")
                .count(),
            1
        );
        drop(replacement);
    }

    #[test]
    fn arm_preflight_refuses_invalid_duplicate_full_and_exhausted_before_input_freezer() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let fixture = ObserverFixture::with_input_freezer(Arc::new(move |_| {
            counter.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }));
        let mut sessions = Vec::new();
        for connection in 1..=MAX_STATEMENT_OBSERVERS as u32 {
            sessions.push(fixture.session(connection, 1));
        }
        let mut invalid = ObserverFixture::arm("bad", 1, "SELECT 1");
        invalid.sql_sha256 = "bad".into();
        assert!(fixture.owner.arm(invalid).is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        for connection in 1..=MAX_STATEMENT_OBSERVERS as u32 {
            fixture
                .owner
                .arm(ObserverFixture::arm(
                    &format!("token-{connection}"),
                    connection,
                    "SELECT 1",
                ))
                .unwrap();
        }
        assert!(
            fixture
                .owner
                .arm(ObserverFixture::arm("token-1", 1, "SELECT 1"))
                .is_err()
        );
        assert!(
            fixture
                .owner
                .arm(ObserverFixture::arm("overflow", 100, "SELECT 1"))
                .is_err()
        );
        fixture.owner.drain("token-1").unwrap();
        fixture.owner.state.lock().unwrap().next_generation = u64::MAX;
        assert!(
            fixture
                .owner
                .arm(ObserverFixture::arm("exhausted", 1, "SELECT 1"))
                .is_err()
        );
        assert_eq!(calls.load(Ordering::Relaxed), MAX_STATEMENT_OBSERVERS);
        assert!(
            !fixture
                .role
                .scope()
                .observations()
                .iter()
                .any(|event| event.event == "hit")
        );
        drop(sessions);
    }

    #[test]
    fn a_compiled_observer_with_no_grant_has_no_owner_or_secret_source() {
        assert!(
            StatementObservationOwner::from_grants(Vec::new())
                .unwrap()
                .is_none()
        );
    }
}
