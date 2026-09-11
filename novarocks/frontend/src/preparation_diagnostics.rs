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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::query_execution::control::StatementToken;
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
