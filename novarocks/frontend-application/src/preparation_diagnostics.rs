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

//! Run-scoped, observation-only preparation diagnostics.
//!
//! The collector is absent unless the process was explicitly launched with a
//! strong control secret. Arming and draining require that secret and one
//! exact run token. The query path only reads the already-armed state; it does
//! not alter planning, caching, retry, resource, or result behavior.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::query_execution::lifecycle_diagnostics::QueryLifecycleConvergenceSnapshot;
use novarocks_query_application::session_control::StatementToken;
use novarocks_types::{QueryExecutionId, QueryId};

pub(crate) const CONTROL_SECRET_ENV: &str = "NOVAROCKS_PREPARATION_DIAGNOSTIC_SECRET";
const MAX_EVENTS_PER_RUN: usize = 100_000;

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub(crate) struct PreparationEvent {
    pub work_id: String,
    pub logical_execution_id: String,
    pub attempt_id: Option<String>,
    pub phase: String,
    pub operation: String,
    pub capability_path: String,
    pub call_count: u64,
    pub elapsed_ns: u64,
    pub io_wait_ns: Option<u64>,
    pub cache_hit: Option<bool>,
    pub outcome: String,
}

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
static ACTIVE_STATEMENT_OBSERVERS: AtomicU64 = AtomicU64::new(0);

/// An observation capability only: neither its lifetime nor its contents
/// control admission, execution, recovery, result visibility, or cleanup.
#[derive(Clone, Debug)]
pub(crate) struct StatementObservationHandle {
    run_token: String,
    generation: u64,
}

struct StatementObserver {
    generation: u64,
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
    fn arm(&mut self, request: StatementObservationArmRequest) -> Result<(), String> {
        if request.connection_id == 0 {
            return Err("statement observation connection id must be nonzero".to_owned());
        }
        if request.run_token.trim().is_empty() || request.run_token.len() > 256 {
            return Err("statement observation token must contain 1..256 bytes".to_owned());
        }
        if self.runs.contains_key(&request.run_token)
            || self
                .runs
                .values()
                .any(|run| run.connection_id == request.connection_id)
        {
            return Err("statement observation token or connection is already armed".to_owned());
        }
        if self.runs.len() >= MAX_STATEMENT_OBSERVERS {
            return Err("statement observation capacity exhausted".to_owned());
        }
        if request.sql_sha256.len() != 64
            || !request
                .sql_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err("statement SQL digest must be lowercase SHA-256 hex".to_owned());
        }
        let mut sql_digest = [0u8; 32];
        for (index, byte) in sql_digest.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&request.sql_sha256[index * 2..index * 2 + 2], 16)
                .expect("validated SHA-256 hex");
        }
        let generation = self
            .next_generation
            .checked_add(1)
            .ok_or_else(|| "statement observation generation exhausted".to_owned())?;
        self.next_generation = generation;
        self.runs.insert(
            request.run_token,
            StatementObserver {
                generation,
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
        if statement.session().session_epoch() == 0 || statement.generation() == 0 {
            return None;
        }
        let digest: [u8; 32] = Sha256::digest(sql.as_bytes()).into();
        let (token, run) = self.runs.iter_mut().find(|(_, run)| {
            run.connection_id == statement.session().connection_id() && run.sql_digest == digest
        })?;
        if run.statement.is_some_and(|bound| bound != statement) {
            return None;
        }
        run.statement = Some(statement);
        Some(StatementObservationHandle {
            run_token: token.clone(),
            generation: run.generation,
        })
    }

    fn bound_statement(&self, statement: StatementToken) -> Option<StatementObservationHandle> {
        self.runs
            .iter()
            .find(|(_, run)| run.statement == Some(statement))
            .map(|(token, run)| StatementObservationHandle {
                run_token: token.clone(),
                generation: run.generation,
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
                    statement.session().connection_id(),
                    statement.session().session_epoch(),
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

fn statement_observers() -> &'static Mutex<StatementObservers> {
    static OBSERVERS: OnceLock<Mutex<StatementObservers>> = OnceLock::new();
    OBSERVERS.get_or_init(|| Mutex::new(StatementObservers::default()))
}

pub(crate) fn arm_statement_observation(
    request: StatementObservationArmRequest,
) -> Result<(), String> {
    if !enabled() {
        return Err("preparation diagnostics are disabled".to_owned());
    }
    let mut state = statement_observers()
        .lock()
        .map_err(|_| "statement observation lock poisoned".to_owned())?;
    state.arm(request)?;
    ACTIVE_STATEMENT_OBSERVERS.store(state.runs.len() as u64, Ordering::Release);
    Ok(())
}

pub(crate) fn peek_statement_observation(
    token: &str,
) -> Result<StatementObservationResponse, String> {
    statement_observers()
        .lock()
        .map_err(|_| "statement observation lock poisoned".to_owned())?
        .response(token)
}

pub(crate) fn drain_statement_observation(
    token: &str,
) -> Result<StatementObservationResponse, String> {
    let mut state = statement_observers()
        .lock()
        .map_err(|_| "statement observation lock poisoned".to_owned())?;
    let response = state.drain(token)?;
    ACTIVE_STATEMENT_OBSERVERS.store(state.runs.len() as u64, Ordering::Release);
    Ok(response)
}

thread_local! {
    static STATEMENT_OBSERVATION: RefCell<Option<StatementObservationHandle>> = const { RefCell::new(None) };
}

pub(crate) struct StatementObservationScope(Option<StatementObservationHandle>);
impl Drop for StatementObservationScope {
    fn drop(&mut self) {
        STATEMENT_OBSERVATION.with(|slot| {
            slot.replace(self.0.take());
        });
    }
}

pub(crate) fn enter_statement_for_sql(
    statement: StatementToken,
    sql: &str,
) -> Option<StatementObservationScope> {
    if ACTIVE_STATEMENT_OBSERVERS.load(Ordering::Acquire) == 0 {
        return None;
    }
    let handle = statement_observers()
        .lock()
        .ok()?
        .bind_statement(statement, sql);
    Some(STATEMENT_OBSERVATION.with(|slot| StatementObservationScope(slot.replace(handle))))
}

pub(crate) fn enter_bound_statement(
    statement: StatementToken,
) -> Option<StatementObservationScope> {
    if ACTIVE_STATEMENT_OBSERVERS.load(Ordering::Acquire) == 0 {
        return None;
    }
    let handle = statement_observers()
        .lock()
        .ok()?
        .bound_statement(statement);
    Some(STATEMENT_OBSERVATION.with(|slot| StatementObservationScope(slot.replace(handle))))
}

pub(crate) fn capture_statement_observation() -> Option<StatementObservationHandle> {
    STATEMENT_OBSERVATION.with(|slot| slot.borrow().clone())
}

pub(crate) fn statement_observations_active() -> bool {
    ACTIVE_STATEMENT_OBSERVERS.load(Ordering::Acquire) != 0
}

pub(crate) fn bind_observed_attempt(
    handle: &StatementObservationHandle,
    execution: QueryExecutionId,
) {
    if let Ok(mut state) = statement_observers().lock() {
        state.bind_attempt(handle, execution);
    }
}

pub(crate) fn publish_observed_convergence(snapshot: &QueryLifecycleConvergenceSnapshot) {
    if ACTIVE_STATEMENT_OBSERVERS.load(Ordering::Acquire) == 0 {
        return;
    }
    if let Ok(mut state) = statement_observers().lock() {
        state.publish(snapshot);
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct DrainResponse {
    pub schema_version: u8,
    pub run_token: String,
    pub events: Vec<PreparationEvent>,
}

struct ArmedCollection {
    generation: u64,
    run_token: String,
    events: Vec<PreparationEvent>,
    overflowed: bool,
}

impl ArmedCollection {
    fn push_with_limit(&mut self, event: PreparationEvent, limit: usize) -> bool {
        if self.events.len() >= limit {
            self.overflowed = true;
            return false;
        }
        self.events.push(event);
        true
    }
}

struct CollectorState {
    secret_digest: Option<[u8; 32]>,
    next_generation: u64,
    armed: Option<ArmedCollection>,
}

impl CollectorState {
    fn authorized(&self, header: Option<&str>) -> bool {
        let Some(secret_digest) = self.secret_digest else {
            return false;
        };
        header
            .and_then(|value| value.strip_prefix("Bearer "))
            .map(|candidate| <[u8; 32]>::from(Sha256::digest(candidate.as_bytes())))
            .is_some_and(|candidate_digest| candidate_digest == secret_digest)
    }

    fn try_arm(&mut self, run_token: String) -> Result<u64, String> {
        if run_token.trim().is_empty() {
            return Err("preparation diagnostic run token must not be empty".to_string());
        }
        if self.secret_digest.is_none() {
            return Err("preparation diagnostics are disabled".to_string());
        }
        if self.armed.is_some() {
            return Err("preparation diagnostics are already armed".to_string());
        }
        let generation = self
            .next_generation
            .checked_add(1)
            .ok_or_else(|| "preparation diagnostic generation exhausted".to_string())?;
        self.next_generation = generation;
        self.armed = Some(ArmedCollection {
            generation,
            run_token,
            events: Vec::new(),
            overflowed: false,
        });
        Ok(generation)
    }

    fn try_drain(&mut self, run_token: &str) -> Result<DrainResponse, String> {
        let armed = self
            .armed
            .as_ref()
            .ok_or_else(|| "preparation diagnostics are not armed".to_string())?;
        if armed.run_token != run_token {
            return Err(
                "preparation diagnostic run token does not match the armed run".to_string(),
            );
        }
        let armed = self.armed.take().expect("checked armed collection");
        if armed.overflowed {
            return Err(format!(
                "preparation diagnostic run exceeded the {MAX_EVENTS_PER_RUN}-event safety bound"
            ));
        }
        Ok(DrainResponse {
            schema_version: 1,
            run_token: armed.run_token,
            events: armed.events,
        })
    }

    fn record_with(
        &mut self,
        generation: u64,
        build: impl FnOnce() -> PreparationEvent,
    ) -> RecordDisposition {
        let Some(armed) = self
            .armed
            .as_mut()
            .filter(|armed| armed.generation == generation)
        else {
            return RecordDisposition::Stale;
        };
        if armed.overflowed {
            return RecordDisposition::Overflowed;
        }
        if armed.push_with_limit(build(), MAX_EVENTS_PER_RUN) {
            RecordDisposition::Accepted
        } else {
            RecordDisposition::Overflowed
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecordDisposition {
    Accepted,
    Overflowed,
    Stale,
}

static COLLECTOR: OnceLock<Mutex<CollectorState>> = OnceLock::new();
static ACTIVE_GENERATION: AtomicU64 = AtomicU64::new(0);

fn collector() -> &'static Mutex<CollectorState> {
    COLLECTOR.get_or_init(|| {
        let secret_digest = std::env::var(CONTROL_SECRET_ENV)
            .ok()
            .filter(|value| value.len() >= 32)
            .map(|value| <[u8; 32]>::from(Sha256::digest(value.as_bytes())));
        Mutex::new(CollectorState {
            secret_digest,
            next_generation: 0,
            armed: None,
        })
    })
}

pub(crate) fn enabled() -> bool {
    collector()
        .lock()
        .expect("preparation diagnostic collector lock poisoned")
        .secret_digest
        .is_some()
}

fn active_generation() -> u64 {
    ACTIVE_GENERATION.load(Ordering::Acquire)
}

pub(crate) fn authorize(header: Option<&str>) -> bool {
    let state = collector()
        .lock()
        .expect("preparation diagnostic collector lock poisoned");
    state.authorized(header)
}

pub(crate) fn arm(run_token: String) -> Result<(), String> {
    let mut state = collector()
        .lock()
        .map_err(|_| "preparation diagnostic collector lock poisoned".to_string())?;
    let generation = state.try_arm(run_token)?;
    ACTIVE_GENERATION.store(generation, Ordering::Release);
    Ok(())
}

pub(crate) fn drain(run_token: &str) -> Result<DrainResponse, String> {
    let mut state = collector()
        .lock()
        .map_err(|_| "preparation diagnostic collector lock poisoned".to_string())?;
    drain_state(&mut state, run_token)
}

fn drain_state(state: &mut CollectorState, run_token: &str) -> Result<DrainResponse, String> {
    let exact_generation = state
        .armed
        .as_ref()
        .filter(|armed| armed.run_token == run_token)
        .map(|armed| armed.generation);
    let response = state.try_drain(run_token);
    if let Some(generation) = exact_generation {
        let _ =
            ACTIVE_GENERATION.compare_exchange(generation, 0, Ordering::AcqRel, Ordering::Acquire);
    }
    response
}

#[derive(Clone)]
struct DiagnosticContext {
    generation: u64,
    work_id: String,
    logical_execution_id: Option<String>,
    logical_execution_locked: bool,
}

thread_local! {
    static CONTEXT: RefCell<Option<DiagnosticContext>> = const { RefCell::new(None) };
}

pub(crate) struct StatementScope {
    previous: Option<DiagnosticContext>,
}

impl Drop for StatementScope {
    fn drop(&mut self) {
        let previous = self.previous.take();
        CONTEXT.with(|slot| {
            slot.replace(previous);
        });
    }
}

pub(crate) fn enter_statement(token: StatementToken) -> Option<StatementScope> {
    let generation = active_generation();
    if generation == 0 {
        return None;
    }
    let work_id = format!(
        "statement:{}:{}:{}",
        token.session().connection_id(),
        token.session().session_epoch(),
        token.generation()
    );
    Some(CONTEXT.with(|slot| {
        let previous = slot.replace(Some(DiagnosticContext {
            generation,
            work_id,
            logical_execution_id: None,
            logical_execution_locked: false,
        }));
        StatementScope { previous }
    }))
}

pub(crate) fn enter_product_work(
    work_id: impl Into<String>,
    logical_execution_id: impl Into<String>,
) -> Option<StatementScope> {
    let generation = active_generation();
    if generation == 0 {
        return None;
    }
    Some(CONTEXT.with(|slot| {
        let previous = slot.replace(Some(DiagnosticContext {
            generation,
            work_id: work_id.into(),
            logical_execution_id: Some(logical_execution_id.into()),
            logical_execution_locked: true,
        }));
        StatementScope { previous }
    }))
}

pub(crate) fn bind_logical_query(query_id: QueryId) {
    // Compilation reservations also reach this hook. A statement observation
    // binds only the execution frozen by Native preparation, which may use a
    // different query identity from the compilation diagnostic scope.
    let generation = active_generation();
    if generation == 0 {
        return;
    }
    CONTEXT.with(|slot| {
        if let Some(context) = slot.borrow_mut().as_mut()
            && context.generation == generation
            && !context.logical_execution_locked
        {
            context.logical_execution_id = Some(format!("query:{query_id}"));
        }
    });
}

pub(crate) fn observe_result<R, E>(
    phase: &'static str,
    operation: &'static str,
    capability_path: &'static str,
    attempt_id: Option<QueryExecutionId>,
    call: impl FnOnce() -> Result<R, E>,
) -> Result<R, E> {
    observe_result_lazy(
        phase,
        || operation.to_string(),
        capability_path,
        attempt_id,
        call,
    )
}

pub(crate) fn observe_result_lazy<R, E>(
    phase: &'static str,
    operation: impl FnOnce() -> String,
    capability_path: &'static str,
    attempt_id: Option<QueryExecutionId>,
    call: impl FnOnce() -> Result<R, E>,
) -> Result<R, E> {
    let generation = active_generation();
    if generation == 0 {
        return call();
    }
    let context = CONTEXT.with(|slot| {
        slot.borrow()
            .as_ref()
            .filter(|context| context.generation == generation)
            .cloned()
    });
    let Some(context) = context else {
        return call();
    };
    let Some(logical_execution_id) = context.logical_execution_id else {
        return call();
    };
    let started = Instant::now();
    let result = call();
    let elapsed_ns = started.elapsed().as_nanos();
    if elapsed_ns == 0 || elapsed_ns > u128::from(u64::MAX) {
        return result;
    }
    if let Ok(mut state) = collector().lock() {
        let disposition = state.record_with(generation, || PreparationEvent {
            work_id: context.work_id,
            logical_execution_id,
            attempt_id: attempt_id.map(|execution| {
                format!(
                    "query={};attempt={}",
                    execution.query_id(),
                    execution.attempt_id().get()
                )
            }),
            phase: phase.to_string(),
            operation: operation(),
            capability_path: capability_path.to_string(),
            call_count: 1,
            elapsed_ns: elapsed_ns as u64,
            io_wait_ns: None,
            cache_hit: None,
            outcome: if result.is_ok() { "success" } else { "failed" }.to_string(),
        });
        if disposition == RecordDisposition::Overflowed {
            let _ = ACTIVE_GENERATION.compare_exchange(
                generation,
                0,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_shape_keeps_attempt_fields_explicit() {
        let event = PreparationEvent {
            work_id: "statement:1:2:3".to_string(),
            logical_execution_id: "query:q".to_string(),
            attempt_id: None,
            phase: "compile".to_string(),
            operation: "optimize".to_string(),
            capability_path: "not-applicable".to_string(),
            call_count: 1,
            elapsed_ns: 1,
            io_wait_ns: None,
            cache_hit: None,
            outcome: "success".to_string(),
        };
        let value = serde_json::to_value(event).expect("serialize diagnostic event");
        assert_eq!(value.as_object().expect("event object").len(), 11);
        assert!(value["attempt_id"].is_null());
    }

    #[test]
    fn disabled_collector_rejects_control_and_authorization() {
        let mut state = CollectorState {
            secret_digest: None,
            next_generation: 0,
            armed: None,
        };
        assert!(!state.authorized(Some("Bearer anything")));
        assert_eq!(
            state.try_arm("run".to_string()).unwrap_err(),
            "preparation diagnostics are disabled"
        );
    }

    #[test]
    fn exact_run_token_owns_one_arm_and_drain_cycle() {
        let secret = "s".repeat(32);
        let mut state = CollectorState {
            secret_digest: Some(Sha256::digest(secret.as_bytes()).into()),
            next_generation: 0,
            armed: None,
        };
        assert!(state.authorized(Some(&format!("Bearer {secret}"))));
        assert!(!state.authorized(Some("Bearer wrong")));
        assert_eq!(state.try_arm("run-a".to_string()).expect("arm run"), 1);
        assert!(state.try_arm("run-b".to_string()).is_err());
        assert!(state.try_drain("run-b").is_err());
        let drained = state.try_drain("run-a").expect("drain exact run");
        assert_eq!(drained.run_token, "run-a");
        assert!(state.armed.is_none());
    }

    #[test]
    fn overflow_is_bounded_and_drain_fails_closed() {
        let event = PreparationEvent {
            work_id: "work".to_string(),
            logical_execution_id: "logical".to_string(),
            attempt_id: None,
            phase: "compile".to_string(),
            operation: "analyze".to_string(),
            capability_path: "not-applicable".to_string(),
            call_count: 1,
            elapsed_ns: 1,
            io_wait_ns: None,
            cache_hit: None,
            outcome: "success".to_string(),
        };
        let mut state = CollectorState {
            secret_digest: Some([7; 32]),
            next_generation: 1,
            armed: Some(ArmedCollection {
                generation: 1,
                run_token: "run".to_string(),
                events: Vec::new(),
                overflowed: false,
            }),
        };
        let armed = state.armed.as_mut().expect("armed run");
        assert!(armed.push_with_limit(event.clone(), 1));
        assert!(!armed.push_with_limit(event, 1));
        assert_eq!(armed.events.len(), 1);
        ACTIVE_GENERATION.store(1, Ordering::Release);
        assert_eq!(
            drain_state(&mut state, "run").unwrap_err(),
            "preparation diagnostic run exceeded the 100000-event safety bound"
        );
        assert!(state.armed.is_none());
        assert_eq!(active_generation(), 0);
    }

    #[test]
    fn late_completion_from_drained_run_cannot_enter_next_run() {
        let mut state = CollectorState {
            secret_digest: Some([9; 32]),
            next_generation: 0,
            armed: None,
        };
        let generation_a = state.try_arm("run-a".to_string()).expect("arm run A");
        let captured_generation = generation_a;
        state.try_drain("run-a").expect("drain run A");
        let generation_b = state.try_arm("run-b".to_string()).expect("arm run B");
        assert!(generation_b > generation_a);

        let operation_built = std::cell::Cell::new(false);
        let disposition = state.record_with(captured_generation, || {
            operation_built.set(true);
            PreparationEvent {
                work_id: "late-work-a".to_string(),
                logical_execution_id: "late-logical-a".to_string(),
                attempt_id: None,
                phase: "compile".to_string(),
                operation: "late-operation-a".to_string(),
                capability_path: "not-applicable".to_string(),
                call_count: 1,
                elapsed_ns: 1,
                io_wait_ns: None,
                cache_hit: None,
                outcome: "success".to_string(),
            }
        });
        assert_eq!(disposition, RecordDisposition::Stale);
        assert!(!operation_built.get());
        assert!(
            state
                .armed
                .as_ref()
                .expect("run B remains armed")
                .events
                .is_empty()
        );
    }
}

#[cfg(test)]
mod statement_observation_tests {
    use super::*;
    use crate::query_execution::lifecycle_diagnostics::{
        RuntimeFilterTerminalRollupSnapshot, RuntimeFilterTerminalRollupUnavailable,
    };
    use novarocks_query_application::session_control::SessionToken;
    use novarocks_types::AttemptId;

    fn arm_request(token: &str, connection_id: u32, sql: &str) -> StatementObservationArmRequest {
        StatementObservationArmRequest {
            run_token: token.to_owned(),
            connection_id,
            sql_sha256: format!("{:x}", Sha256::digest(sql.as_bytes())),
        }
    }

    fn statement(connection: u32, epoch: u64, generation: u64) -> StatementToken {
        StatementToken::new(SessionToken::new(connection, epoch), generation)
    }

    fn execution(high: i64, low: i64, attempt: u64) -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(high, low), AttemptId::new(attempt).unwrap()).unwrap()
    }

    fn snapshot(execution_id: QueryExecutionId) -> QueryLifecycleConvergenceSnapshot {
        QueryLifecycleConvergenceSnapshot {
            execution_id,
            error_source: None,
            primary_error: None,
            runtime_filter: RuntimeFilterTerminalRollupSnapshot::Unavailable(
                RuntimeFilterTerminalRollupUnavailable::TerminalOutcomesIncomplete,
            ),
        }
    }

    #[test]
    fn statement_observation_matches_exact_connection_sql_and_statement_owner() {
        let mut state = StatementObservers::default();
        state
            .arm(arm_request("selected", 7, "SELECT k FROM t"))
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
        assert_eq!(response.work_id.as_deref(), Some("statement:7:9:12"));
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
            .arm(arm_request("selected", 7, "SELECT k FROM t"))
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
    fn statement_observation_parallel_tokens_are_bounded_and_drain_invalidates_handles() {
        let mut state = StatementObservers::default();
        assert!(state.arm(arm_request("zero", 0, "SELECT 1")).is_err());
        for connection in 1..=MAX_STATEMENT_OBSERVERS as u32 {
            state
                .arm(arm_request(
                    &format!("run-{connection}"),
                    connection,
                    "SELECT 1",
                ))
                .unwrap();
        }
        assert!(state.arm(arm_request("run-1", 100, "SELECT 1")).is_err());
        assert!(state.arm(arm_request("another", 1, "SELECT 1")).is_err());
        assert!(state.arm(arm_request("overflow", 100, "SELECT 1")).is_err());
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
        state.arm(arm_request("run-1", 1, "SELECT 1")).unwrap();
        state.bind_attempt(&a, execution(5, 43, 2));
        state.publish(&snapshot(execution(5, 41, 1)));
        assert!(state.response("run-1").unwrap().work_id.is_none());
        assert!(state.response("run-1").unwrap().execution_id.is_none());
        assert!(state.response("run-1").unwrap().snapshot.is_none());
        assert_eq!(state.runs.len(), MAX_STATEMENT_OBSERVERS);
        state.next_generation = u64::MAX;
        state.drain("run-1").unwrap();
        assert!(state.arm(arm_request("exhausted", 1, "SELECT 1")).is_err());
    }

    #[test]
    fn statement_observation_scope_restores_previous_context_without_cross_statement_leak() {
        assert!(capture_statement_observation().is_none());
        let outer = StatementObservationHandle {
            run_token: "outer".to_owned(),
            generation: 1,
        };
        let outer_scope =
            STATEMENT_OBSERVATION.with(|slot| StatementObservationScope(slot.replace(Some(outer))));
        assert_eq!(capture_statement_observation().unwrap().run_token, "outer");
        let unrelated_scope =
            STATEMENT_OBSERVATION.with(|slot| StatementObservationScope(slot.replace(None)));
        assert!(capture_statement_observation().is_none());
        drop(unrelated_scope);
        assert_eq!(capture_statement_observation().unwrap().run_token, "outer");
        drop(outer_scope);
        assert!(capture_statement_observation().is_none());
    }
}
