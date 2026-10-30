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

//! Test-only transparent Iceberg REST proxy for publication acceptance tests.
//!
//! The proxy owns no catalog state and exposes no catalog extension.  It only
//! recognizes standard REST publication and discovery requests in order to
//! consume one runner-owned fault token. The real REST Catalog remains the
//! only authority for every create, commit, ref, object, and package outcome.

use anyhow::{Context, Result};
use axum::body::{Body, Bytes};
use axum::extract::{Path as AxumPath, Request, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::borrow::Cow;
use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const MAX_PROXY_BODY_BYTES: usize = 64 * 1024 * 1024;
const MAX_PROXY_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
const MAX_FAULT_TRACE_EVENTS: usize = 256;

#[derive(Clone, Debug)]
pub struct FixtureConfig {
    #[allow(dead_code)]
    pub listen: SocketAddr,
    pub downstream: String,
}

pub struct FixtureHandle {
    uri: String,
    next_fault: Arc<Mutex<NextFaultState>>,
    mutations: Arc<Mutex<BTreeMap<(String, String), MutationCounts>>>,
    traffic: Arc<Mutex<TrafficCounters>>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    stopped: tokio::sync::watch::Sender<bool>,
    thread: Option<JoinHandle<()>>,
}

#[derive(Clone)]
pub struct FixtureControl {
    uri: String,
    client: reqwest::blocking::Client,
    next_fault: Arc<Mutex<NextFaultState>>,
    mutations: Arc<Mutex<BTreeMap<(String, String), MutationCounts>>>,
    traffic: Arc<Mutex<TrafficCounters>>,
}

pub struct FixtureFaultGuard {
    control: FixtureControl,
    arm_id: String,
    fault: PublicationFault,
    cleared: bool,
}

pub struct FixtureFaultEvidence {
    events: Vec<String>,
    trace: Value,
}

impl FixtureFaultEvidence {
    pub fn trace(&self) -> &Value {
        &self.trace
    }

    pub fn summary(&self) -> String {
        self.events.join(" -> ")
    }
}

#[derive(Clone)]
struct AppState {
    stopped: tokio::sync::watch::Receiver<bool>,
    downstream: String,
    client: reqwest::Client,
    next_fault: Arc<Mutex<NextFaultState>>,
    next_fault_sequence: Arc<AtomicU64>,
    mutations: Arc<Mutex<BTreeMap<(String, String), MutationCounts>>>,
    traffic: Arc<Mutex<TrafficCounters>>,
}

/// Cumulative traffic forwarded to the real REST Catalog. Fixture control
/// requests and requests refused before dispatch are excluded.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct TrafficCounters {
    pub requests: u64,
    pub request_body_bytes: u64,
    pub response_body_bytes: u64,
    pub table_commit_requests: u64,
    pub table_commit_roundtrip_nanos: u64,
    pub by_method: BTreeMap<String, u64>,
    pub by_status: BTreeMap<u16, u64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MutationCounts {
    pub stage_create_forwarded: usize,
    pub stage_create_succeeded: usize,
    pub table_commit_forwarded: usize,
    pub table_commit_succeeded: usize,
    pub table_commit_pending: usize,
    pub table_load_while_commit_pending: usize,
    pub other_forwarded: usize,
    pub other_succeeded: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TargetMutationAction {
    StageCreate,
    TableCommit,
    Other,
}

#[derive(Default)]
struct NextFaultState {
    armed: Option<ArmedNextFault>,
    status: Option<ConsumedNextFault>,
    trace_sequence: u64,
    trace: VecDeque<FaultTraceEvent>,
}

#[derive(Debug, Clone, Serialize)]
struct FaultTraceEvent {
    sequence: u64,
    arm_id: String,
    event: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    request: Option<MatchedRequestObservation>,
}

/// Only public request routing facts, never headers, query strings or bodies.
#[derive(Debug, Clone, Serialize)]
struct MatchedRequestObservation {
    method: String,
    namespace: Option<String>,
    table: Option<String>,
    entered_unix_nanos: Option<u64>,
    matched_unix_nanos: Option<u64>,
}

fn observed_unix_nanos() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| u64::try_from(elapsed.as_nanos()).ok())
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum PublicationAction {
    StageCreate,
    TableCommit,
    NamespaceList,
    TableLoad,
    CatalogMutation,
}

impl PublicationAction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StageCreate => "stage-create",
            Self::TableCommit => "table-commit",
            Self::NamespaceList => "namespace-list",
            Self::TableLoad => "table-load",
            Self::CatalogMutation => "catalog-mutation",
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum PublicationFault {
    BeforeDispatch,
    BeforeDispatchForbidden,
    BeforeDispatchHoldForConcurrentShell,
    BeforeRequirementCheckHoldForConcurrentShell,
    AfterCommitBeforeResponse,
    AfterCommitHoldForFrontendKill,
    IncompleteDiscovery,
    CorruptPackage,
    StreamResponse,
}

impl PublicationFault {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BeforeDispatch => "before-dispatch",
            Self::BeforeDispatchForbidden => "before-dispatch-forbidden",
            Self::BeforeDispatchHoldForConcurrentShell => {
                "before-dispatch-hold-for-concurrent-shell"
            }
            Self::BeforeRequirementCheckHoldForConcurrentShell => {
                "before-requirement-check-hold-for-concurrent-shell"
            }
            Self::AfterCommitBeforeResponse => "after-commit-before-response",
            Self::AfterCommitHoldForFrontendKill => "after-commit-hold-for-frontend-kill",
            Self::IncompleteDiscovery => "incomplete-discovery",
            Self::CorruptPackage => "corrupt-package",
            Self::StreamResponse => "stream-response",
        }
    }
}

#[derive(Debug, Clone)]
struct ArmedNextFault {
    arm_id: String,
    action: PublicationAction,
    fault: PublicationFault,
    target: Option<ExactTableTarget>,
    release: Arc<tokio::sync::Notify>,
    released: Arc<AtomicBool>,
}

#[derive(Debug, Clone)]
struct ConsumedNextFault {
    arm_id: String,
    entered: bool,
    release: Arc<tokio::sync::Notify>,
    released: Arc<AtomicBool>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ExactTableTarget {
    namespace: String,
    table: String,
}

#[derive(Deserialize, Serialize)]
struct ArmNextFaultRequest {
    action: PublicationAction,
    fault: PublicationFault,
    #[serde(default)]
    target: Option<ExactTableTarget>,
}

#[derive(Deserialize, Serialize)]
struct ClearNextFaultResponse {
    entered: bool,
}

#[derive(Deserialize, Serialize)]
struct ArmNextFaultResponse {
    arm_id: String,
}

impl FixtureHandle {
    pub fn start(downstream: String) -> Result<Self> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")
            .context("reserve publication catalog fixture listener")?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let next_fault = Arc::new(Mutex::new(NextFaultState::default()));
        let mutations = Arc::new(Mutex::new(BTreeMap::new()));
        let traffic = Arc::new(Mutex::new(TrafficCounters::default()));
        let (stopped, stopped_rx) = tokio::sync::watch::channel(false);
        let state = AppState {
            stopped: stopped_rx,
            downstream: downstream.trim_end_matches('/').to_string(),
            client: reqwest::Client::builder().no_proxy().build()?,
            next_fault: Arc::clone(&next_fault),
            next_fault_sequence: Arc::new(AtomicU64::new(1)),
            mutations: Arc::clone(&mutations),
            traffic: Arc::clone(&traffic),
        };
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let thread = std::thread::Builder::new()
            .name("publication-catalog-fixture".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build publication catalog fixture runtime");
                runtime.block_on(async move {
                    let listener = tokio::net::TcpListener::from_std(listener)
                        .expect("adopt publication catalog fixture listener");
                    ready_tx
                        .send(())
                        .expect("signal publication catalog fixture ready");
                    axum::serve(listener, router(state))
                        .with_graceful_shutdown(async move {
                            let _ = shutdown_rx.await;
                        })
                        .await
                        .expect("serve publication catalog fixture");
                });
            })?;
        if let Err(error) = ready_rx.recv_timeout(Duration::from_secs(5)) {
            let _ = stopped.send(true);
            let _ = shutdown.send(());
            let joined = thread.join().is_ok();
            anyhow::bail!(
                "wait for publication catalog fixture readiness: {error}; owner_joined={joined}"
            );
        }
        Ok(Self {
            uri: format!("http://{address}"),
            next_fault,
            mutations,
            traffic,
            shutdown: Some(shutdown),
            stopped,
            thread: Some(thread),
        })
    }

    pub fn shutdown(&mut self) -> Result<()> {
        let mut failures = Vec::new();
        // Stop handlers and slow response streams before graceful listener join.
        // Notify carries one permit if release races the request's first poll.
        let _ = self.stopped.send(true);
        {
            let next = self.next_fault.lock().expect("publication fault mutex");
            if let Some(armed) = &next.armed {
                armed.released.store(true, Ordering::Release);
                armed.release.notify_one();
            }
            if let Some(status) = &next.status {
                status.released.store(true, Ordering::Release);
                status.release.notify_one();
            }
        }
        if let Some(shutdown) = self.shutdown.take()
            && shutdown.send(()).is_err()
        {
            failures.push("publication fixture exited before its shutdown request");
        }
        if let Some(thread) = self.thread.take()
            && thread.join().is_err()
        {
            failures.push("publication fixture owner panicked during actual join");
        }
        if failures.is_empty() {
            Ok(())
        } else {
            anyhow::bail!(failures.join("; "))
        }
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    pub fn control(&self) -> Result<FixtureControl> {
        Ok(FixtureControl {
            uri: self.uri.clone(),
            client: reqwest::blocking::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(5))
                .build()?,
            next_fault: Arc::clone(&self.next_fault),
            mutations: Arc::clone(&self.mutations),
            traffic: Arc::clone(&self.traffic),
        })
    }
}

impl FixtureControl {
    pub fn traffic_snapshot(&self) -> TrafficCounters {
        self.traffic.lock().expect("catalog traffic mutex").clone()
    }

    pub fn mutation_counts(&self, namespace: &str, table: &str) -> MutationCounts {
        self.mutations
            .lock()
            .expect("publication mutation mutex")
            .get(&(namespace.to_string(), table.to_string()))
            .copied()
            .unwrap_or_default()
    }

    pub fn arm_next(&self, action: &str, fault: &str) -> Result<FixtureFaultGuard> {
        self.arm_next_target(action, fault, None)
    }

    pub fn arm_next_for_table(
        &self,
        action: &str,
        fault: &str,
        namespace: &str,
        table: &str,
    ) -> Result<FixtureFaultGuard> {
        self.arm_next_target(
            action,
            fault,
            Some(ExactTableTarget {
                namespace: namespace.to_owned(),
                table: table.to_owned(),
            }),
        )
    }

    fn arm_next_target(
        &self,
        action: &str,
        fault: &str,
        target: Option<ExactTableTarget>,
    ) -> Result<FixtureFaultGuard> {
        let action = parse_action(action)?;
        let fault = parse_fault(fault)?;
        let response: ArmNextFaultResponse = self
            .client
            .post(format!("{}/_fixture/publication-faults/next", self.uri))
            .json(&ArmNextFaultRequest {
                action,
                fault,
                target,
            })
            .send()
            .context("arm publication catalog next-action fault")?
            .error_for_status()
            .context("publication catalog next-action fault was rejected")?
            .json()
            .context("decode publication catalog next-action fault receipt")?;
        Ok(FixtureFaultGuard {
            control: self.clone(),
            arm_id: response.arm_id,
            fault,
            cleared: false,
        })
    }

    fn clear_arm(&self, arm_id: &str) -> Result<bool> {
        let response: ClearNextFaultResponse = self
            .client
            .delete(format!(
                "{}/_fixture/publication-faults/next/{arm_id}",
                self.uri
            ))
            .send()
            .context("clear publication catalog next-action fault")?
            .error_for_status()
            .context("publication catalog next-action fault cleanup was rejected")?
            .json()
            .context("decode publication catalog next-action fault cleanup")?;
        Ok(response.entered)
    }

    fn wait_until_entered(&self, arm_id: &str, deadline: Instant) -> Result<()> {
        loop {
            let entered = self
                .next_fault
                .lock()
                .expect("publication fault mutex")
                .status
                .as_ref()
                .filter(|status| status.arm_id == arm_id)
                .is_some_and(|status| status.entered);
            if entered {
                return Ok(());
            }
            if Instant::now() >= deadline {
                anyhow::bail!(
                    "timed out waiting for publication catalog fault {arm_id} to reach its hold"
                );
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    fn wait_for_trace_event(&self, arm_id: &str, expected: &str, deadline: Instant) -> Result<()> {
        loop {
            if self
                .next_fault
                .lock()
                .expect("publication fault mutex")
                .trace
                .iter()
                .any(|event| event.arm_id == arm_id && event.event == expected)
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                anyhow::bail!(
                    "timed out waiting for publication catalog fault {arm_id} trace event {expected}"
                );
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    fn trace_events(&self, arm_id: &str) -> Vec<String> {
        let mut events = self
            .next_fault
            .lock()
            .expect("publication fault mutex")
            .trace
            .iter()
            .filter(|event| event.arm_id == arm_id)
            .cloned()
            .collect::<Vec<_>>();
        events.sort_by_key(|event| event.sequence);
        events.into_iter().map(|event| event.event).collect()
    }
}

fn parse_action(value: &str) -> Result<PublicationAction> {
    match value {
        "stage-create" => Ok(PublicationAction::StageCreate),
        "table-commit" => Ok(PublicationAction::TableCommit),
        "namespace-list" => Ok(PublicationAction::NamespaceList),
        "table-load" => Ok(PublicationAction::TableLoad),
        "catalog-mutation" => Ok(PublicationAction::CatalogMutation),
        other => anyhow::bail!("unknown publication catalog action `{other}`"),
    }
}

fn parse_fault(value: &str) -> Result<PublicationFault> {
    match value {
        "before-dispatch" => Ok(PublicationFault::BeforeDispatch),
        "before-dispatch-forbidden" => Ok(PublicationFault::BeforeDispatchForbidden),
        "before-dispatch-hold-for-concurrent-shell" => {
            Ok(PublicationFault::BeforeDispatchHoldForConcurrentShell)
        }
        "before-requirement-check-hold-for-concurrent-shell" => {
            Ok(PublicationFault::BeforeRequirementCheckHoldForConcurrentShell)
        }
        "after-commit-before-response" => Ok(PublicationFault::AfterCommitBeforeResponse),
        "after-commit-hold-for-frontend-kill" => {
            Ok(PublicationFault::AfterCommitHoldForFrontendKill)
        }
        "incomplete-discovery" => Ok(PublicationFault::IncompleteDiscovery),
        "corrupt-package" => Ok(PublicationFault::CorruptPackage),
        "stream-response" => Ok(PublicationFault::StreamResponse),
        other => anyhow::bail!("unknown publication catalog fault `{other}`"),
    }
}

impl FixtureFaultGuard {
    /// Snapshot the exact arm trace before cleanup consumes the guard. This is
    /// evidence, not a substitute for finish's actual-exit verification.
    pub fn trace(&self) -> Value {
        let state = self
            .control
            .next_fault
            .lock()
            .expect("publication fault mutex");
        Value::Array(
            state
                .trace
                .iter()
                .filter(|event| event.arm_id == self.arm_id)
                .map(|event| json!(event))
                .collect(),
        )
    }

    pub fn wait_until_entered(&self, deadline: Instant) -> Result<()> {
        self.control.wait_until_entered(&self.arm_id, deadline)
    }

    pub fn wait_until_downstream_successful_hold(&self, deadline: Instant) -> Result<()> {
        self.control.wait_for_trace_event(
            &self.arm_id,
            "response-held-after-downstream-success",
            deadline,
        )
    }

    pub fn release(&mut self) -> Result<bool> {
        if self.cleared {
            return Ok(true);
        }
        let entered = self.control.clear_arm(&self.arm_id)?;
        self.cleared = true;
        Ok(entered)
    }

    pub fn finish(mut self) -> Result<FixtureFaultEvidence> {
        let entered = self.release()?;
        if !entered {
            anyhow::bail!(
                "publication catalog next-action fault {} was not consumed by its matching REST request",
                self.arm_id
            );
        }
        let terminal_event = match self.fault {
            PublicationFault::BeforeDispatch => "known-not-dispatched",
            PublicationFault::BeforeDispatchForbidden => "forbidden-not-dispatched",
            PublicationFault::BeforeDispatchHoldForConcurrentShell
            | PublicationFault::BeforeRequirementCheckHoldForConcurrentShell => {
                "downstream-response"
            }
            PublicationFault::AfterCommitBeforeResponse => {
                "response-dropped-after-downstream-success"
            }
            // Killing the FE closes its client connection. The proxy handler
            // may be canceled before it can record a post-release response;
            // the pre-kill successful hold and control release are the proof.
            PublicationFault::AfterCommitHoldForFrontendKill => {
                "response-held-after-downstream-success"
            }
            PublicationFault::IncompleteDiscovery => "discovery-response-replaced",
            PublicationFault::CorruptPackage => "package-response-corrupted",
            PublicationFault::StreamResponse => "response-stream-exited",
        };
        self.control.wait_for_trace_event(
            &self.arm_id,
            terminal_event,
            Instant::now() + Duration::from_secs(5),
        )?;
        Ok(FixtureFaultEvidence {
            trace: self.trace(),
            events: self.control.trace_events(&self.arm_id),
        })
    }
}

impl Drop for FixtureFaultGuard {
    fn drop(&mut self) {
        if !self.cleared {
            let _ = self.release();
        }
    }
}

impl Drop for FixtureHandle {
    fn drop(&mut self) {
        if let Err(error) = self.shutdown() {
            eprintln!("publication fixture cleanup failed: {error:#}");
        }
    }
}

pub async fn serve(config: FixtureConfig) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .context("bind publication catalog fixture listener")?;
    let (_stopped, stopped_rx) = tokio::sync::watch::channel(false);
    let state = AppState {
        stopped: stopped_rx,
        downstream: config.downstream.trim_end_matches('/').to_string(),
        client: reqwest::Client::builder().no_proxy().build()?,
        next_fault: Arc::new(Mutex::new(NextFaultState::default())),
        next_fault_sequence: Arc::new(AtomicU64::new(1)),
        mutations: Arc::new(Mutex::new(BTreeMap::new())),
        traffic: Arc::new(Mutex::new(TrafficCounters::default())),
    };
    axum::serve(listener, router(state))
        .await
        .context("serve publication catalog fixture")
}

fn router(state: AppState) -> Router {
    Router::new()
        .route(
            "/_fixture/catalog-traffic",
            axum::routing::get(catalog_traffic),
        )
        .route("/_fixture/publication-faults/next", post(arm_next_fault))
        .route(
            "/_fixture/publication-faults/next/:arm_id",
            axum::routing::delete(clear_next_fault),
        )
        .fallback(any(dispatch))
        .with_state(state)
}

async fn catalog_traffic(State(state): State<AppState>) -> Json<TrafficCounters> {
    let snapshot = state.traffic.lock().expect("catalog traffic mutex").clone();
    Json(snapshot)
}

async fn arm_next_fault(
    State(state): State<AppState>,
    Json(request): Json<ArmNextFaultRequest>,
) -> Response {
    if let Some(target) = &request.target
        && (request.action != PublicationAction::TableLoad
            || request.fault != PublicationFault::CorruptPackage
            || target.namespace.is_empty()
            || target.table.is_empty()
            || target.namespace.contains('/')
            || target.table.contains('/'))
    {
        return wire_error(
            StatusCode::BAD_REQUEST,
            "fixture-target",
            "invalid exact table-load fault target",
        );
    }
    let arm_id = format!(
        "publication-fault-{}",
        state.next_fault_sequence.fetch_add(1, Ordering::Relaxed)
    );
    let mut next = state.next_fault.lock().expect("publication fault mutex");
    if next.armed.is_some() || next.status.is_some() {
        return wire_error(
            StatusCode::CONFLICT,
            "fixture-busy",
            "a publication catalog fault token is already active",
        );
    }
    next.armed = Some(ArmedNextFault {
        arm_id: arm_id.clone(),
        action: request.action,
        fault: request.fault,
        target: request.target,
        release: Arc::new(tokio::sync::Notify::new()),
        released: Arc::new(AtomicBool::new(false)),
    });
    record_fault_event_locked(&mut next, &arm_id, "armed");
    json_response(StatusCode::OK, json!({"arm_id": arm_id}))
}

async fn clear_next_fault(
    State(state): State<AppState>,
    AxumPath(arm_id): AxumPath<String>,
) -> Response {
    let mut next = state.next_fault.lock().expect("publication fault mutex");
    let (entered, release, released) = if let Some(status) = next
        .status
        .as_ref()
        .filter(|status| status.arm_id == arm_id)
    {
        (
            status.entered,
            Arc::clone(&status.release),
            Arc::clone(&status.released),
        )
    } else if let Some(armed) = next.armed.as_ref().filter(|armed| armed.arm_id == arm_id) {
        (
            false,
            Arc::clone(&armed.release),
            Arc::clone(&armed.released),
        )
    } else {
        return wire_error(
            StatusCode::NOT_FOUND,
            "fixture-token",
            "publication catalog fault token is unknown",
        );
    };
    record_fault_event_locked(&mut next, &arm_id, "control-release");
    next.status = None;
    if next
        .armed
        .as_ref()
        .is_some_and(|armed| armed.arm_id == arm_id)
    {
        next.armed = None;
    }
    drop(next);
    released.store(true, Ordering::Release);
    release.notify_one();
    json_response(StatusCode::OK, json!({"entered": entered}))
}

async fn dispatch(State(state): State<AppState>, request: Request) -> Response {
    let entered_unix_nanos = observed_unix_nanos();
    let (parts, body) = request.into_parts();
    let bytes = match axum::body::to_bytes(body, MAX_PROXY_BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(error) => return temporary_failure(error.to_string()),
    };
    let action = standard_catalog_action(&parts.method, parts.uri.path(), &bytes);
    let mutation_target = mutation_target(&parts.method, action, parts.uri.path(), &bytes);
    if action == Some(PublicationAction::TableLoad)
        && let Some((namespace, table)) = exact_table_target(parts.uri.path())
    {
        let mut mutations = state.mutations.lock().expect("publication mutation mutex");
        let counts = mutations
            .entry((namespace.to_owned(), table.to_owned()))
            .or_default();
        if counts.table_commit_pending > 0 {
            counts.table_load_while_commit_pending += 1;
        }
    }
    let fault = match action {
        Some(action) => match take_matching_fault(
            &state,
            action,
            &parts.method,
            parts.uri.path(),
            entered_unix_nanos,
        ) {
            Ok(fault) => fault,
            Err(_) => {
                return wire_error(
                    StatusCode::BAD_REQUEST,
                    "fixture-target",
                    "invalid encoded REST table target",
                );
            }
        },
        None => None,
    };
    if let Some(armed) = fault.as_ref()
        && armed.fault == PublicationFault::BeforeDispatch
    {
        // The fixture proves it did not forward this request. Use a typed
        // non-5xx REST response so the standard client can truthfully classify
        // the operation as known-not-dispatched rather than a transport
        // ambiguity; no private catalog protocol participates.
        record_fault_event(&state, armed, "known-not-dispatched");
        return known_not_dispatched("publication REST request rejected before dispatch");
    }
    if let Some(armed) = fault.as_ref()
        && armed.fault == PublicationFault::BeforeDispatchForbidden
    {
        // A standard REST 403 is a definite authorization rejection: the
        // server refused the request before any catalog side effect, and the
        // fixture proves it by never forwarding the request.
        record_fault_event(&state, armed, "forbidden-not-dispatched");
        return forbidden_not_dispatched("publication REST request forbidden before dispatch");
    }
    if let Some(armed) = fault.as_ref()
        && matches!(
            armed.fault,
            PublicationFault::BeforeDispatchHoldForConcurrentShell
                | PublicationFault::BeforeRequirementCheckHoldForConcurrentShell
        )
    {
        record_fault_event(&state, armed, "before-requirement-check-hold");
        let mut stopped = state.stopped.clone();
        tokio::select! {
            _ = armed.release.notified() => {},
            _ = stopped.changed() => {},
        }
        record_fault_event(&state, armed, "before-requirement-check-release");
    }
    if *state.stopped.borrow() {
        return temporary_failure("publication fixture stopped before forwarding");
    }
    if let Some(armed) = fault.as_ref() {
        record_fault_event(&state, armed, "request-forwarded");
    }
    if let Some((action, namespace, table)) = mutation_target.as_ref() {
        record_mutation(&state, *action, namespace, table, false);
    }
    let method = parts.method.clone();
    let request_body_bytes = bytes.len() as u64;
    let forwarded_at = Instant::now();
    let (response, response_body_bytes) =
        proxy_request(&state, parts.method, parts.uri, parts.headers, bytes).await;
    if let Some((TargetMutationAction::TableCommit, namespace, table)) = mutation_target.as_ref() {
        let mut mutations = state.mutations.lock().expect("publication mutation mutex");
        let counts = mutations
            .get_mut(&(namespace.to_owned(), table.to_owned()))
            .expect("forwarded commit counter");
        counts.table_commit_pending = counts
            .table_commit_pending
            .checked_sub(1)
            .expect("one forwarded commit exit");
    }
    let roundtrip_nanos = u64::try_from(forwarded_at.elapsed().as_nanos())
        .expect("catalog request duration exceeds u64 nanoseconds");
    record_traffic(
        &state,
        &method,
        response.status(),
        request_body_bytes,
        response_body_bytes,
        mutation_target
            .as_ref()
            .is_some_and(|(action, _, _)| *action == TargetMutationAction::TableCommit),
        roundtrip_nanos,
    );
    if response.status().is_success()
        && let Some((action, namespace, table)) = mutation_target.as_ref()
    {
        record_mutation(&state, *action, namespace, table, true);
    }
    if let Some(armed) = fault.as_ref() {
        record_fault_event(&state, armed, "downstream-response");
        if response.status().is_success() {
            record_fault_event(&state, armed, "downstream-success");
        }
    }
    if let Some(armed) = fault
        && response.status().is_success()
    {
        match armed.fault {
            PublicationFault::AfterCommitBeforeResponse => {
                record_fault_event(&state, &armed, "response-dropped-after-downstream-success");
                return temporary_failure(
                    "publication REST response was lost after downstream success",
                );
            }
            PublicationFault::AfterCommitHoldForFrontendKill => {
                record_fault_event(&state, &armed, "response-held-after-downstream-success");
                let mut stopped = state.stopped.clone();
                tokio::select! {
                    _ = armed.release.notified() => {},
                    _ = stopped.changed() => {},
                }
                record_fault_event(&state, &armed, "response-dropped-after-downstream-success");
                return temporary_failure("publication REST response released after frontend kill");
            }
            PublicationFault::IncompleteDiscovery => {
                // A valid empty list means discovery completed with no
                // namespaces. The MV contract needs an actual standard REST
                // read failure so SPI classifies this catalog as Incomplete.
                record_fault_event(&state, &armed, "discovery-response-replaced");
                return temporary_failure("catalog discovery read failed");
            }
            PublicationFault::CorruptPackage => {
                record_fault_event(&state, &armed, "package-response-corrupted");
                return response_with_headers(
                    StatusCode::OK,
                    HeaderMap::new(),
                    Bytes::from_static(b"{corrupt-package"),
                );
            }
            PublicationFault::StreamResponse => {
                return stream_response(state.clone(), armed, response).await;
            }
            PublicationFault::BeforeDispatch | PublicationFault::BeforeDispatchForbidden => {
                unreachable!("returned before dispatch")
            }
            PublicationFault::BeforeDispatchHoldForConcurrentShell
            | PublicationFault::BeforeRequirementCheckHoldForConcurrentShell => {}
        }
    }
    response
}

/// Dropping the HTTP body is an observable exit, including a client deadline.
struct StreamExit {
    state: AppState,
    armed: ArmedNextFault,
}
impl Drop for StreamExit {
    fn drop(&mut self) {
        record_fault_event(&self.state, &self.armed, "response-stream-exited");
    }
}

async fn stream_response(state: AppState, armed: ArmedNextFault, response: Response) -> Response {
    use tokio_stream::StreamExt;
    let (mut parts, body) = response.into_parts();
    let bytes = match axum::body::to_bytes(body, MAX_PROXY_RESPONSE_BYTES).await {
        Ok(bytes) => bytes,
        Err(error) => return temporary_failure(format!("prepare response stream: {error}")),
    };
    parts.headers.remove(axum::http::header::CONTENT_LENGTH);
    record_fault_event(&state, &armed, "response-stream-started");
    let exit = Arc::new(StreamExit {
        state: state.clone(),
        armed: armed.clone(),
    });
    let stream = tokio_stream::iter(bytes.to_vec()).then(move |byte| {
        let exit = Arc::clone(&exit);
        let mut stopped = state.stopped.clone();
        let release = Arc::clone(&armed.release);
        let released = Arc::clone(&armed.released);
        async move {
            if released.load(Ordering::Acquire) || *stopped.borrow() {
                return Err(std::io::Error::other("publication stream stopped"));
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(50)) => {
                    if released.load(Ordering::Acquire) || *stopped.borrow() {
                        return Err(std::io::Error::other("publication stream stopped"));
                    }
                    record_fault_event(&exit.state, &exit.armed, "response-stream-chunk");
                    Ok::<Bytes, std::io::Error>(Bytes::from(vec![byte]))
                },
                _ = stopped.changed() => Err(std::io::Error::other("publication fixture stopped")),
                _ = release.notified() => Err(std::io::Error::other("publication stream released")),
            }
        }
    });
    Response::from_parts(parts, Body::from_stream(stream))
}

fn standard_catalog_action(method: &Method, path: &str, body: &[u8]) -> Option<PublicationAction> {
    if !path.contains("/v1/") {
        return None;
    }
    if method == Method::GET && path.ends_with("/v1/namespaces") {
        return Some(PublicationAction::NamespaceList);
    }
    if method == Method::GET && path.contains("/tables/") {
        return Some(PublicationAction::TableLoad);
    }
    if matches!(method, &Method::DELETE | &Method::PUT | &Method::PATCH) {
        return Some(PublicationAction::CatalogMutation);
    }
    if method != Method::POST {
        return None;
    }
    let value: Value = serde_json::from_slice(body).ok()?;
    if path.ends_with("/tables") && value.get("stage-create").and_then(Value::as_bool) == Some(true)
    {
        return Some(PublicationAction::StageCreate);
    }
    if path.contains("/tables/")
        && value.get("requirements").is_some()
        && value.get("updates").is_some()
    {
        return Some(PublicationAction::TableCommit);
    }
    Some(PublicationAction::CatalogMutation)
}

fn exact_table_target(path: &str) -> Option<(&str, &str)> {
    let (_, rest) = path.split_once("/v1/namespaces/")?;
    let (namespace, table) = rest.split_once("/tables/")?;
    if namespace.is_empty() || namespace.contains('/') || table.is_empty() || table.contains('/') {
        return None;
    }
    Some((namespace, table))
}

/// Split the wire path first, then decode each component exactly once.
fn decoded_exact_table_target(path: &str) -> Result<Option<(Cow<'_, str>, Cow<'_, str>)>> {
    let Some((namespace, table)) = exact_table_target(path) else {
        return Ok(None);
    };
    fn decode(component: &str) -> Result<Cow<'_, str>> {
        // percent-encoding preserves malformed escapes literally. Refuse those
        // before using its strict UTF-8 decoder; this loop performs no decoding.
        let bytes = component.as_bytes();
        for (index, byte) in bytes.iter().enumerate() {
            if *byte == b'%'
                && !(bytes.get(index + 1).is_some_and(u8::is_ascii_hexdigit)
                    && bytes.get(index + 2).is_some_and(u8::is_ascii_hexdigit))
            {
                anyhow::bail!("invalid REST target percent escape");
            }
        }
        let value = percent_encoding::percent_decode_str(component)
            .decode_utf8()
            .context("REST target is not UTF-8")?;
        if value.contains('/') {
            anyhow::bail!("encoded REST target separator is not a name component");
        }
        Ok(value)
    }
    Ok(Some((decode(namespace)?, decode(table)?)))
}

fn mutation_target(
    method: &Method,
    action: Option<PublicationAction>,
    path: &str,
    body: &[u8],
) -> Option<(TargetMutationAction, String, String)> {
    if !matches!(
        method,
        &Method::POST | &Method::PUT | &Method::PATCH | &Method::DELETE
    ) {
        return None;
    }
    let remainder = path.split_once("/v1/namespaces/")?.1;
    let (namespace, table_path) = remainder.split_once("/tables")?;
    if namespace.is_empty() || namespace.contains('/') {
        return None;
    }
    let (kind, table) = if table_path.is_empty() && method == Method::POST {
        let value: Value = serde_json::from_slice(body).ok()?;
        let table = value.get("name")?.as_str()?.to_string();
        let kind = if action == Some(PublicationAction::StageCreate) {
            TargetMutationAction::StageCreate
        } else {
            TargetMutationAction::Other
        };
        (kind, table)
    } else {
        let table = table_path.strip_prefix('/')?.to_string();
        let kind = if method == Method::POST && action == Some(PublicationAction::TableCommit) {
            TargetMutationAction::TableCommit
        } else {
            TargetMutationAction::Other
        };
        (kind, table)
    };
    if table.is_empty() || table.contains('/') {
        return None;
    }
    Some((kind, namespace.to_string(), table))
}

fn record_mutation(
    state: &AppState,
    action: TargetMutationAction,
    namespace: &str,
    table: &str,
    succeeded: bool,
) {
    let mut mutations = state.mutations.lock().expect("publication mutation mutex");
    let counts = mutations
        .entry((namespace.to_string(), table.to_string()))
        .or_default();
    match (action, succeeded) {
        (TargetMutationAction::StageCreate, false) => counts.stage_create_forwarded += 1,
        (TargetMutationAction::StageCreate, true) => counts.stage_create_succeeded += 1,
        (TargetMutationAction::TableCommit, false) => {
            counts.table_commit_forwarded += 1;
            counts.table_commit_pending += 1;
        }
        (TargetMutationAction::TableCommit, true) => counts.table_commit_succeeded += 1,
        (TargetMutationAction::Other, false) => counts.other_forwarded += 1,
        (TargetMutationAction::Other, true) => counts.other_succeeded += 1,
    }
}

fn take_matching_fault(
    state: &AppState,
    action: PublicationAction,
    method: &Method,
    path: &str,
    entered_unix_nanos: Option<u64>,
) -> Result<Option<ArmedNextFault>> {
    let mut next = state.next_fault.lock().expect("publication fault mutex");
    let Some(armed) = next.armed.as_ref() else {
        return Ok(None);
    };
    if armed.action != action {
        return Ok(None);
    }
    if let Some(target) = &armed.target {
        let Some((namespace, table)) = decoded_exact_table_target(path)? else {
            return Ok(None);
        };
        if namespace.as_ref() != target.namespace.as_str()
            || table.as_ref() != target.table.as_str()
        {
            return Ok(None);
        }
    }
    let matched_unix_nanos = observed_unix_nanos();
    // Consume the arm before the request is dispatched. In particular, a
    // definite OCC conflict may cause the product to issue one fresh attempt;
    // that second standard request must not re-enter this one-shot hold.
    let armed = next.armed.take().expect("matching arm exists");
    next.status = Some(ConsumedNextFault {
        arm_id: armed.arm_id.clone(),
        entered: true,
        release: Arc::clone(&armed.release),
        released: Arc::clone(&armed.released),
    });
    record_fault_event_locked(&mut next, &armed.arm_id, "matched");
    // Only the actual matched request allocates diagnostic routing strings.
    let target = exact_table_target(path);
    next.trace
        .back_mut()
        .expect("the matched event was just recorded")
        .request = Some(MatchedRequestObservation {
        method: method.as_str().to_owned(),
        namespace: target.map(|(namespace, _)| namespace.to_owned()),
        table: target.map(|(_, table)| table.to_owned()),
        entered_unix_nanos,
        matched_unix_nanos,
    });
    Ok(Some(armed))
}

fn record_fault_event(state: &AppState, armed: &ArmedNextFault, event: &str) {
    let mut next = state.next_fault.lock().expect("publication fault mutex");
    record_fault_event_locked(&mut next, &armed.arm_id, event);
}

fn record_fault_event_locked(state: &mut NextFaultState, arm_id: &str, event: &str) {
    state.trace_sequence = state.trace_sequence.saturating_add(1);
    while state.trace.len() >= MAX_FAULT_TRACE_EVENTS {
        state.trace.pop_front();
    }
    state.trace.push_back(FaultTraceEvent {
        sequence: state.trace_sequence,
        arm_id: arm_id.to_string(),
        event: event.to_string(),
        request: None,
    });
}

fn record_traffic(
    state: &AppState,
    method: &Method,
    status: StatusCode,
    request_body_bytes: u64,
    response_body_bytes: u64,
    is_table_commit: bool,
    roundtrip_nanos: u64,
) {
    let mut traffic = state.traffic.lock().expect("catalog traffic mutex");
    traffic.requests += 1;
    traffic.request_body_bytes += request_body_bytes;
    traffic.response_body_bytes += response_body_bytes;
    if is_table_commit {
        traffic.table_commit_requests += 1;
        traffic.table_commit_roundtrip_nanos += roundtrip_nanos;
    }
    *traffic
        .by_method
        .entry(method.as_str().to_string())
        .or_default() += 1;
    *traffic.by_status.entry(status.as_u16()).or_default() += 1;
}

async fn proxy_request(
    state: &AppState,
    method: Method,
    uri: axum::http::Uri,
    headers: HeaderMap,
    bytes: Bytes,
) -> (Response, u64) {
    let url = format!(
        "{}{}",
        state.downstream,
        uri.path_and_query()
            .map(|value| value.as_str())
            .unwrap_or("/")
    );
    let mut outbound = state.client.request(method, url).body(bytes);
    for (name, value) in &headers {
        if name != axum::http::header::HOST && name != axum::http::header::CONTENT_LENGTH {
            outbound = outbound.header(name, value);
        }
    }
    let mut stopped = state.stopped.clone();
    if *stopped.borrow() {
        return (
            temporary_failure("publication fixture stopped before downstream dispatch"),
            0,
        );
    }
    let response = match tokio::select! {
        result = outbound.send() => result,
        _ = stopped.changed() => return (temporary_failure("publication fixture stopped before downstream headers"), 0),
    } {
        Ok(response) => response,
        Err(error) => {
            return (
                temporary_failure(format!("downstream REST request failed: {error}")),
                0,
            );
        }
    };
    let status = response.status();
    let headers = response.headers().clone();
    if headers
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|length| length > MAX_PROXY_RESPONSE_BYTES)
    {
        return (
            temporary_failure("downstream REST response exceeds publication proxy limit"),
            0,
        );
    }
    if *stopped.borrow() {
        return (
            temporary_failure("publication fixture stopped before downstream body"),
            0,
        );
    }
    let bytes = match tokio::select! {
        result = response.bytes() => result,
        _ = stopped.changed() => return (temporary_failure("publication fixture stopped during downstream body"), 0),
    } {
        Ok(bytes) => bytes,
        Err(error) => {
            return (
                temporary_failure(format!("read downstream REST response: {error}")),
                0,
            );
        }
    };
    if bytes.len() > MAX_PROXY_RESPONSE_BYTES {
        return (
            temporary_failure("downstream REST response exceeds publication proxy limit"),
            0,
        );
    }
    let response_body_bytes = bytes.len() as u64;
    (
        response_with_headers(status, headers, bytes),
        response_body_bytes,
    )
}

fn response_with_headers(status: StatusCode, headers: HeaderMap, bytes: Bytes) -> Response {
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = status;
    for (name, value) in &headers {
        if name != axum::http::header::CONTENT_LENGTH
            && name != axum::http::header::TRANSFER_ENCODING
        {
            response.headers_mut().insert(name, value.clone());
        }
    }
    response
}

fn json_response(status: StatusCode, value: Value) -> Response {
    (status, Json(value)).into_response()
}

fn wire_error(status: StatusCode, kind: &str, message: impl Into<String>) -> Response {
    json_response(
        status,
        json!({"error": {"kind": kind, "message": message.into()}}),
    )
}

fn temporary_failure(message: impl Into<String>) -> Response {
    wire_error(StatusCode::SERVICE_UNAVAILABLE, "ambiguous", message)
}

fn known_not_dispatched(message: impl Into<String>) -> Response {
    wire_error(StatusCode::BAD_REQUEST, "known-not-dispatched", message)
}

/// A standard Iceberg REST `ErrorModel` 403 for a request never forwarded.
fn forbidden_not_dispatched(message: impl Into<String>) -> Response {
    json_response(
        StatusCode::FORBIDDEN,
        json!({
            "error": {
                "message": message.into(),
                "type": "ForbiddenException",
                "code": StatusCode::FORBIDDEN.as_u16(),
            }
        }),
    )
}

#[cfg(test)]
mod tests {

    /// Consume the complete bounded request before the downstream fixture replies.
    /// TCP may split either headers or body across arbitrary reads.
    fn consume_downstream_request(socket: &mut std::net::TcpStream) {
        use std::io::{BufRead, Read};
        let mut reader = std::io::BufReader::new(socket);
        let mut content_length = 0;
        let mut header_bytes = 0;
        loop {
            let mut line = String::new();
            let count = reader.read_line(&mut line).unwrap();
            assert!(count > 0, "Downstream request ended before its headers");
            header_bytes += count;
            assert!(
                header_bytes <= 16 * 1024,
                "Fixture request headers exceed their bound"
            );
            if line == "\r\n" {
                break;
            }
            if let Some((name, value)) = line.split_once(':')
                && name.eq_ignore_ascii_case("content-length")
            {
                content_length = value.trim().parse::<usize>().unwrap();
            }
        }
        assert!(
            content_length <= 16 * 1024,
            "Fixture request body exceeds its bound"
        );
        let mut body = vec![0; content_length];
        reader.read_exact(&mut body).unwrap();
    }
    use super::*;

    #[test]
    fn recognizes_only_standard_stage_create_and_table_commit_requests() {
        assert_eq!(
            standard_catalog_action(
                &Method::POST,
                "/v1/namespaces/ns/tables",
                br#"{"stage-create":true}"#,
            ),
            Some(PublicationAction::StageCreate)
        );
        assert_eq!(
            standard_catalog_action(
                &Method::POST,
                "/v1/namespaces/ns/tables/t",
                br#"{"requirements":[],"updates":[]}"#,
            ),
            Some(PublicationAction::TableCommit)
        );
        assert_eq!(
            standard_catalog_action(
                &Method::POST,
                "/extensions/private-action/publish",
                br#"{}"#,
            ),
            None
        );
        assert_eq!(
            standard_catalog_action(&Method::GET, "/v1/namespaces", br#""#),
            Some(PublicationAction::NamespaceList)
        );
        assert_eq!(
            standard_catalog_action(&Method::GET, "/v1/namespaces/ns/tables/t", br#""#),
            Some(PublicationAction::TableLoad)
        );
        assert_eq!(
            mutation_target(
                &Method::POST,
                Some(PublicationAction::StageCreate),
                "/v1/namespaces/ns/tables",
                br#"{"name":"mv","stage-create":true}"#,
            ),
            Some((TargetMutationAction::StageCreate, "ns".into(), "mv".into()))
        );
        assert_eq!(
            mutation_target(
                &Method::POST,
                Some(PublicationAction::TableCommit),
                "/v1/namespaces/ns/tables/mv",
                br#"{"requirements":[],"updates":[]}"#,
            ),
            Some((TargetMutationAction::TableCommit, "ns".into(), "mv".into()))
        );
        assert_eq!(
            mutation_target(
                &Method::POST,
                None,
                "/v1/namespaces/ns/tables/mv",
                br#"{"unexpected":"mutation"}"#,
            ),
            Some((TargetMutationAction::Other, "ns".into(), "mv".into()))
        );
    }

    #[test]
    fn one_shot_fault_only_consumes_its_matching_standard_action() {
        let (_stopped, stopped) = tokio::sync::watch::channel(false);
        let state = AppState {
            stopped,
            downstream: "http://example.invalid".to_string(),
            client: reqwest::Client::builder().no_proxy().build().unwrap(),
            next_fault: Arc::new(Mutex::new(NextFaultState {
                armed: Some(ArmedNextFault {
                    arm_id: "one".to_string(),
                    action: PublicationAction::TableCommit,
                    fault: PublicationFault::AfterCommitBeforeResponse,
                    target: None,
                    release: Arc::new(tokio::sync::Notify::new()),
                    released: Arc::new(AtomicBool::new(false)),
                }),
                status: None,
                ..NextFaultState::default()
            })),
            next_fault_sequence: Arc::new(AtomicU64::new(1)),
            mutations: Arc::new(Mutex::new(BTreeMap::new())),
            traffic: Arc::new(Mutex::new(TrafficCounters::default())),
        };
        assert!(
            take_matching_fault(
                &state,
                PublicationAction::StageCreate,
                &Method::POST,
                "/v1/namespaces/ns/tables/t",
                None
            )
            .unwrap()
            .is_none()
        );
        let consumed = take_matching_fault(
            &state,
            PublicationAction::TableCommit,
            &Method::POST,
            "/v1/namespaces/ns/tables/t",
            None,
        )
        .unwrap()
        .expect("matching fault is consumed");
        assert_eq!(consumed.arm_id, "one");
        assert_eq!(consumed.fault, PublicationFault::AfterCommitBeforeResponse);
        assert!(
            take_matching_fault(
                &state,
                PublicationAction::TableCommit,
                &Method::POST,
                "/v1/namespaces/ns/tables/t",
                None
            )
            .unwrap()
            .is_none()
        );
        let trace = &state.next_fault.lock().unwrap().trace;
        assert_eq!(trace.len(), 1);
        assert_eq!(trace[0].sequence, 1);
        assert_eq!(trace[0].event, "matched");
    }

    #[test]
    fn exact_path_components_decode_once_and_reject_invalid_encoding_or_separators() {
        let target = decoded_exact_table_target("/v1/namespaces/owned%20ns/tables/a%20b")
            .unwrap()
            .unwrap();
        assert_eq!((target.0.as_ref(), target.1.as_ref()), ("owned ns", "a b"));
        let percent = decoded_exact_table_target("/v1/namespaces/ns/tables/a%2520b")
            .unwrap()
            .unwrap();
        assert_eq!(percent.1, "a%20b");
        let plus = decoded_exact_table_target("/v1/namespaces/ns/tables/a+b")
            .unwrap()
            .unwrap();
        assert_eq!(plus.1, "a+b");
        for path in [
            "/v1/namespaces/ns/tables/a%",
            "/v1/namespaces/ns/tables/a%2",
            "/v1/namespaces/ns/tables/a%GG",
            "/v1/namespaces/ns/tables/a%FF",
            "/v1/namespaces/ns/tables/a%2Fb",
            "/v1/namespaces/ns%2Fforeign/tables/a",
        ] {
            assert!(
                decoded_exact_table_target(path).is_err(),
                "accepted {path:?}"
            );
        }
    }

    #[test]
    fn targeted_proxy_distinguishes_space_from_literal_percent_and_keeps_arm_after_bad_wire_names()
    {
        use std::io::Write;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let downstream = std::thread::spawn(move || {
            for _ in 0..4 {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                consume_downstream_request(&mut socket);
                let body = r#"{"metadata":"original"}"#;
                write!(
                    socket,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                )
                .unwrap();
            }
        });
        let mut fixture = FixtureHandle::start(format!("http://{address}")).unwrap();
        let control = fixture.control().unwrap();
        let client = reqwest::blocking::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();
        let base = format!("{}/v1/namespaces/owned_ns/tables", fixture.uri());
        let space = control
            .arm_next_for_table("table-load", "corrupt-package", "owned_ns", "a b")
            .unwrap();
        assert_eq!(
            client
                .get(format!("{base}/a%2520b"))
                .send()
                .unwrap()
                .text()
                .unwrap(),
            r#"{"metadata":"original"}"#
        );
        assert!(
            !space
                .trace()
                .as_array()
                .unwrap()
                .iter()
                .any(|event| event["event"] == "matched")
        );
        assert_eq!(
            client
                .get(format!("{base}/a%20b"))
                .send()
                .unwrap()
                .text()
                .unwrap(),
            "{corrupt-package"
        );
        assert_eq!(
            space.finish().unwrap().summary(),
            "armed -> matched -> request-forwarded -> downstream-response -> downstream-success -> package-response-corrupted -> control-release"
        );
        let percent = control
            .arm_next_for_table("table-load", "corrupt-package", "owned_ns", "a%20b")
            .unwrap();
        for malformed in ["a%", "a%GG", "a%FF", "a%2Fb"] {
            assert_eq!(
                client
                    .get(format!("{base}/{malformed}"))
                    .send()
                    .unwrap()
                    .status(),
                StatusCode::BAD_REQUEST
            );
            assert!(
                !percent
                    .trace()
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|event| event["event"] == "matched")
            );
        }
        assert_eq!(
            client
                .get(format!("{base}/a%20b"))
                .send()
                .unwrap()
                .text()
                .unwrap(),
            r#"{"metadata":"original"}"#
        );
        assert!(
            !percent
                .trace()
                .as_array()
                .unwrap()
                .iter()
                .any(|event| event["event"] == "matched")
        );
        assert_eq!(
            client
                .get(format!("{base}/a%2520b"))
                .send()
                .unwrap()
                .text()
                .unwrap(),
            "{corrupt-package"
        );
        assert_eq!(
            percent.finish().unwrap().summary(),
            "armed -> matched -> request-forwarded -> downstream-response -> downstream-success -> package-response-corrupted -> control-release"
        );
        assert_eq!(control.traffic_snapshot().requests, 4);
        fixture.shutdown().unwrap();
        downstream.join().unwrap();
    }

    #[test]
    fn targeted_read_fault_forwards_other_tables_and_namespaces_without_consuming_arm() {
        use std::io::Write;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let downstream = std::thread::spawn(move || {
            for _ in 0..4 {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                consume_downstream_request(&mut socket);
                let body = r#"{"metadata":"original"}"#;
                write!(
                    socket,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                )
                .unwrap();
            }
        });
        let mut fixture = FixtureHandle::start(format!("http://{address}")).unwrap();
        let control = fixture.control().unwrap();
        let guard = control
            .arm_next_for_table("table-load", "corrupt-package", "owned_ns", "a_mv")
            .unwrap();
        let client = reqwest::blocking::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();
        for path in [
            "/v1/namespaces/owned_ns/tables/z_source",
            "/v1/namespaces/foreign_ns/tables/a_mv",
        ] {
            let response = client
                .get(format!("{}{path}", fixture.uri()))
                .send()
                .unwrap();
            assert_eq!(response.text().unwrap(), r#"{"metadata":"original"}"#);
            let trace = guard.trace();
            assert!(
                !trace
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|event| event["event"] == "matched")
            );
        }
        let exact = format!("{}/v1/namespaces/owned_ns/tables/a_mv", fixture.uri());
        assert_eq!(
            client.get(&exact).send().unwrap().text().unwrap(),
            "{corrupt-package"
        );
        assert_eq!(
            client.get(&exact).send().unwrap().text().unwrap(),
            r#"{"metadata":"original"}"#
        );
        let evidence = guard.finish().unwrap();
        assert_eq!(
            evidence.summary(),
            "armed -> matched -> request-forwarded -> downstream-response -> downstream-success -> package-response-corrupted -> control-release"
        );
        let trace = evidence.trace();
        let matched = trace
            .as_array()
            .unwrap()
            .iter()
            .filter(|event| event["event"] == "matched")
            .collect::<Vec<_>>();
        assert_eq!(matched.len(), 1);
        assert_eq!(matched[0]["request"]["namespace"], "owned_ns");
        assert_eq!(matched[0]["request"]["table"], "a_mv");
        assert_eq!(control.traffic_snapshot().requests, 4);
        fixture.shutdown().unwrap();
        downstream.join().unwrap();
    }

    #[test]
    fn matched_read_trace_keeps_actual_routing_and_times_without_request_secrets() {
        use std::io::Write;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let downstream = std::thread::spawn(move || {
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                consume_downstream_request(&mut socket);
                let body = r#"{"metadata":"original"}"#;
                write!(
                    socket,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                )
                .unwrap();
            }
        });
        let mut fixture = FixtureHandle::start(format!("http://{address}")).unwrap();
        let control = fixture.control().unwrap();
        let guard = control.arm_next("table-load", "corrupt-package").unwrap();
        let client = reqwest::blocking::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();
        let first = client
            .get(format!(
                "{}/v1/namespaces/ns_a/tables/a_mv?credential=query-secret",
                fixture.uri()
            ))
            .header("authorization", "Bearer header-secret")
            .send()
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(first.text().unwrap(), "{corrupt-package");
        let second = client
            .get(format!(
                "{}/v1/namespaces/ns_b/tables/z_source",
                fixture.uri()
            ))
            .send()
            .unwrap();
        assert_eq!(second.text().unwrap(), r#"{"metadata":"original"}"#);
        let evidence = guard.finish().unwrap();
        assert_eq!(
            evidence.summary(),
            "armed -> matched -> request-forwarded -> downstream-response -> downstream-success -> package-response-corrupted -> control-release"
        );
        let observed_trace = evidence.trace();
        let matched = observed_trace
            .as_array()
            .unwrap()
            .iter()
            .filter(|event| event["event"] == "matched")
            .collect::<Vec<_>>();
        assert_eq!(matched.len(), 1);
        let request = &matched[0]["request"];
        assert_eq!(request["method"], "GET");
        assert_eq!(request["namespace"], "ns_a");
        assert_eq!(request["table"], "a_mv");
        assert!(request["entered_unix_nanos"].as_u64().is_some());
        assert!(request["matched_unix_nanos"].as_u64().is_some());
        let trace = evidence.trace().to_string();
        for excluded in [
            "query-secret",
            "header-secret",
            "authorization",
            "credential",
            "ns_b",
            "z_source",
        ] {
            assert!(!trace.contains(excluded));
        }
        assert_eq!(control.traffic_snapshot().requests, 2);
        fixture.shutdown().unwrap();
        downstream.join().unwrap();
    }

    #[test]
    fn parses_explicit_pre_requirement_hold() {
        assert_eq!(
            parse_fault("before-requirement-check-hold-for-concurrent-shell").unwrap(),
            PublicationFault::BeforeRequirementCheckHoldForConcurrentShell
        );
    }

    #[test]
    fn records_forward_success_and_response_drop_for_one_arm() {
        use std::io::{Read, Write};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let downstream_address = listener.local_addr().unwrap();
        let downstream = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut request = [0_u8; 4096];
            let read = stream.read(&mut request).unwrap();
            assert!(
                String::from_utf8_lossy(&request[..read])
                    .contains("POST /v1/namespaces/ns/tables/t")
            );
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
                .unwrap();
        });

        let fixture = FixtureHandle::start(format!("http://{downstream_address}")).unwrap();
        let control = fixture.control().unwrap();
        let guard = control
            .arm_next("table-commit", "after-commit-before-response")
            .unwrap();
        let response = reqwest::blocking::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .post(format!("{}/v1/namespaces/ns/tables/t", fixture.uri()))
            .json(&json!({"requirements": [], "updates": []}))
            .send()
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let evidence = guard.finish().unwrap();
        let counts = control.mutation_counts("ns", "t");
        assert_eq!(counts.table_commit_forwarded, 1);
        assert_eq!(counts.table_commit_succeeded, 1);
        let traffic = control.traffic_snapshot();
        assert_eq!(traffic.requests, 1);
        assert_eq!(traffic.by_method.get("POST"), Some(&1));
        assert_eq!(traffic.by_status.get(&200), Some(&1));
        assert!(traffic.request_body_bytes > 0);
        assert_eq!(traffic.response_body_bytes, 2);
        assert_eq!(traffic.table_commit_requests, 1);
        assert!(traffic.table_commit_roundtrip_nanos > 0);
        let wire_traffic: TrafficCounters =
            reqwest::blocking::get(format!("{}/_fixture/catalog-traffic", fixture.uri()))
                .unwrap()
                .json()
                .unwrap();
        assert_eq!(wire_traffic, traffic);
        assert_eq!(
            evidence.summary(),
            "armed -> matched -> request-forwarded -> downstream-response -> downstream-success -> response-dropped-after-downstream-success -> control-release"
        );
        downstream.join().unwrap();
    }

    #[test]
    fn parses_forbidden_before_dispatch_fault() {
        let fault = parse_fault("before-dispatch-forbidden").unwrap();
        assert_eq!(fault, PublicationFault::BeforeDispatchForbidden);
        assert_eq!(fault.as_str(), "before-dispatch-forbidden");
        assert_eq!(
            parse_fault("before-dispatch").unwrap(),
            PublicationFault::BeforeDispatch
        );
    }

    /// Arms one before-dispatch fault, sends one matching request and
    /// returns its status, decoded body and the fault trace. The downstream
    /// listener never accepts, so any forwarded request would be observable.
    fn answer_before_dispatch(
        action: &str,
        fault: &str,
        path: &str,
        body: Value,
    ) -> (StatusCode, Value, String, TrafficCounters, MutationCounts) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let downstream_address = listener.local_addr().unwrap();
        let fixture = FixtureHandle::start(format!("http://{downstream_address}")).unwrap();
        let control = fixture.control().unwrap();
        let guard = control.arm_next(action, fault).unwrap();
        let response = reqwest::blocking::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .post(format!("{}{path}", fixture.uri()))
            .json(&body)
            .send()
            .unwrap();
        let status = response.status();
        let body: Value = response.json().unwrap();
        let evidence = guard.finish().unwrap();
        assert!(
            matches!(
                listener.accept(),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
            ),
            "a before-dispatch fault must not open a downstream connection"
        );
        (
            status,
            body,
            evidence.summary(),
            control.traffic_snapshot(),
            control.mutation_counts("ns", "t"),
        )
    }

    #[test]
    fn forbidden_fault_answers_403_without_forwarding_the_mutation() {
        for (action, path, body) in [
            (
                "table-commit",
                "/v1/namespaces/ns/tables/t",
                json!({"requirements": [], "updates": []}),
            ),
            (
                "stage-create",
                "/v1/namespaces/ns/tables",
                json!({"name": "t", "stage-create": true}),
            ),
        ] {
            let (status, body, trace, traffic, counts) =
                answer_before_dispatch(action, "before-dispatch-forbidden", path, body);
            assert_eq!(status, StatusCode::FORBIDDEN, "{action}");
            assert_eq!(body.pointer("/error/code"), Some(&json!(403)), "{body}");
            assert_eq!(
                body.pointer("/error/type"),
                Some(&json!("ForbiddenException")),
                "{body}"
            );
            assert!(
                body.pointer("/error/message")
                    .and_then(Value::as_str)
                    .is_some_and(|message| !message.is_empty())
            );
            assert_eq!(
                trace,
                "armed -> matched -> forbidden-not-dispatched -> control-release"
            );
            assert_eq!(traffic, TrafficCounters::default());
            assert_eq!(counts, MutationCounts::default());
        }
    }

    #[test]
    fn before_dispatch_fault_still_answers_400_without_forwarding() {
        let (status, body, trace, traffic, counts) = answer_before_dispatch(
            "stage-create",
            "before-dispatch",
            "/v1/namespaces/ns/tables",
            json!({"name": "t", "stage-create": true}),
        );
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            body.pointer("/error/kind"),
            Some(&json!("known-not-dispatched"))
        );
        assert_eq!(
            trace,
            "armed -> matched -> known-not-dispatched -> control-release"
        );
        assert_eq!(traffic, TrafficCounters::default());
        assert_eq!(counts, MutationCounts::default());
    }

    #[test]
    fn publication_stream_exits_after_client_deadline_and_fixture_join() {
        use std::io::Write;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let downstream = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            consume_downstream_request(&mut socket);
            let body = " ".repeat(128);
            write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
        });
        let mut fixture = FixtureHandle::start(format!("http://{address}")).unwrap();
        let control = fixture.control().unwrap();
        let guard = control
            .arm_next("catalog-mutation", "stream-response")
            .unwrap();
        let response = reqwest::blocking::Client::builder()
            .no_proxy()
            .timeout(Duration::from_millis(300))
            .build()
            .unwrap()
            .delete(format!("{}/v1/namespaces/ns/tables/t", fixture.uri()))
            .send()
            .unwrap();
        assert!(
            response.bytes().is_err(),
            "slow body must exceed whole-request deadline"
        );
        let evidence = guard.finish().unwrap().summary();
        assert!(evidence.contains("response-stream-started"));
        assert!(evidence.contains("response-stream-chunk"));
        assert!(evidence.contains("response-stream-exited"));
        assert_eq!(control.traffic_snapshot().requests, 1);
        fixture.shutdown().unwrap();
        downstream.join().unwrap();
    }

    #[test]
    fn publication_fixture_shutdown_releases_a_request_before_forwarding() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let mut fixture =
            FixtureHandle::start(format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let control = fixture.control().unwrap();
        let guard = control
            .arm_next("table-commit", "before-dispatch-hold-for-concurrent-shell")
            .unwrap();
        let uri = fixture.uri().to_owned();
        let query = std::thread::spawn(move || {
            reqwest::blocking::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(3))
                .build()
                .unwrap()
                .post(format!("{uri}/v1/namespaces/ns/tables/t"))
                .json(&json!({"requirements": [], "updates": []}))
                .send()
        });
        guard
            .wait_until_entered(Instant::now() + Duration::from_secs(2))
            .unwrap();
        fixture.shutdown().unwrap();
        let _ = query.join().expect("publication query actual exit");
    }

    #[test]
    fn publication_fixture_shutdown_cancels_downstream_body_and_joins() {
        use std::io::Write;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (headers_tx, headers_rx) = std::sync::mpsc::sync_channel(1);
        let (stop_tx, stop_rx) = std::sync::mpsc::sync_channel(1);
        let downstream = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            consume_downstream_request(&mut socket);
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1024\r\nConnection: close\r\n\r\n{")
                .unwrap();
            headers_tx.send(()).unwrap();
            let _ = stop_rx.recv_timeout(Duration::from_secs(3));
        });
        let mut fixture = FixtureHandle::start(format!("http://{address}")).unwrap();
        let uri = fixture.uri().to_owned();
        let query = std::thread::spawn(move || {
            reqwest::blocking::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(3))
                .build()
                .unwrap()
                .post(format!("{uri}/v1/namespaces/ns/tables/t"))
                .json(&json!({"requirements": [], "updates": []}))
                .send()
                .unwrap()
                .status()
        });
        headers_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        fixture.shutdown().unwrap();
        assert_eq!(
            query.join().expect("publication query actual exit"),
            StatusCode::SERVICE_UNAVAILABLE
        );
        let _ = stop_tx.send(());
        downstream.join().unwrap();
    }

    #[test]
    fn publication_unused_arm_is_cleared_before_next_step() {
        let fixture = FixtureHandle::start("http://127.0.0.1:9".to_owned()).unwrap();
        let control = fixture.control().unwrap();
        let mut first = control.arm_next("table-commit", "stream-response").unwrap();
        assert!(
            !first.release().unwrap(),
            "unused arm must not claim a matched request"
        );
        let mut second = control
            .arm_next("catalog-mutation", "before-dispatch")
            .unwrap();
        assert!(!second.release().unwrap());
    }

    #[test]
    fn publication_observes_exact_table_load_during_original_pending_commit() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let downstream = std::thread::spawn(move || {
            let (mut commit, _) = listener.accept().unwrap();
            commit
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let mut bytes = [0; 4096];
            let count = commit.read(&mut bytes).unwrap();
            assert!(
                String::from_utf8_lossy(&bytes[..count])
                    .starts_with("POST /v1/namespaces/ns/tables/t")
            );
            entered_tx.send(()).unwrap();
            let (mut read, _) = listener.accept().unwrap();
            read.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
            let count = read.read(&mut bytes).unwrap();
            assert!(
                String::from_utf8_lossy(&bytes[..count])
                    .starts_with("GET /v1/namespaces/ns/tables/t")
            );
            read.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
                .unwrap();
            release_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            commit
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
                .unwrap();
        });
        let fixture = FixtureHandle::start(format!("http://{address}")).unwrap();
        let control = fixture.control().unwrap();
        let uri = fixture.uri().to_owned();
        let commit = std::thread::spawn(move || {
            reqwest::blocking::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(3))
                .build()
                .unwrap()
                .post(format!("{uri}/v1/namespaces/ns/tables/t"))
                .json(&json!({"requirements": [], "updates": []}))
                .send()
                .unwrap()
                .status()
        });
        entered_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        let client = reqwest::blocking::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();
        assert_eq!(
            client
                .get(format!("{}/v1/namespaces/ns/tables/t", fixture.uri()))
                .send()
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            control
                .mutation_counts("ns", "t")
                .table_load_while_commit_pending,
            1
        );
        assert_eq!(control.mutation_counts("ns", "t").table_commit_pending, 1);
        release_tx.send(()).unwrap();
        assert_eq!(commit.join().unwrap(), StatusCode::OK);
        assert_eq!(control.mutation_counts("ns", "t").table_commit_pending, 0);
        downstream.join().unwrap();
    }
}
