// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Test-only fence-aware proxy for Iceberg REST Catalog acceptance tests.
//!
//! The in-process locks only prevent needless request overlap. SQLite is the
//! durable authority: every decision re-reads and validates the operation row
//! in a transaction, so fixture restart never weakens fencing.

use anyhow::{Context, Result};
use axum::body::{Body, Bytes};
use axum::extract::{Path as AxumPath, Request, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, delete, post};
use axum::{Json, Router};
use opendal::Operator;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;

const CAPABILITY: &str = "fenced-staged-publication";
const MAX_BODY_BYTES: usize = 64 * 1024;
const MAX_PROXY_BODY_BYTES: usize = 64 * 1024 * 1024;
const MAX_CLEANUP_ITEMS: usize = 512;
const MAX_CLEANUP_PATH_BYTES: usize = 2 * 1024;
const MAX_CLEANUP_TOTAL_PATH_BYTES: usize = 48 * 1024;

#[derive(Clone, Debug)]
pub(crate) struct FixtureConfig {
    // Used by the standalone fixture binary; the in-process runner reserves
    // and adopts its listener directly.
    #[allow(dead_code)]
    pub(crate) listen: SocketAddr,
    pub(crate) downstream: String,
    pub(crate) sqlite_path: PathBuf,
}

#[allow(dead_code)] // The standalone fixture binary uses `serve` instead.
pub(crate) struct FixtureHandle {
    uri: String,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

#[derive(Clone)]
#[allow(dead_code)] // The standalone fixture binary does not expose runner controls.
pub(crate) struct FixtureControl {
    uri: String,
    client: reqwest::blocking::Client,
}

#[allow(dead_code)] // The standalone fixture binary does not expose runner controls.
pub(crate) struct FixtureFaultGuard {
    control: FixtureControl,
    arm_id: String,
    cleared: bool,
}

#[allow(dead_code)] // The standalone fixture binary uses `serve` instead.
impl FixtureHandle {
    pub(crate) fn start(downstream: String, sqlite_path: PathBuf) -> Result<Self> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")
            .context("reserve fenced catalog fixture listener")?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let config = FixtureConfig {
            listen: address,
            downstream,
            sqlite_path,
        };
        let state = build_state(&config)?;
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let thread = std::thread::Builder::new()
            .name("fenced-catalog-fixture".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build fenced catalog fixture runtime");
                runtime.block_on(async move {
                    let listener = tokio::net::TcpListener::from_std(listener)
                        .expect("adopt fenced catalog fixture listener");
                    ready_tx
                        .send(())
                        .expect("signal fenced catalog fixture ready");
                    axum::serve(listener, router(state))
                        .with_graceful_shutdown(async move {
                            let _ = shutdown_rx.await;
                        })
                        .await
                        .expect("serve fenced catalog fixture");
                });
            })?;
        ready_rx
            .recv_timeout(Duration::from_secs(5))
            .context("wait for fenced catalog fixture readiness")?;
        Ok(Self {
            uri: format!("http://{address}"),
            shutdown: Some(shutdown),
            thread: Some(thread),
        })
    }

    pub(crate) fn uri(&self) -> &str {
        &self.uri
    }

    pub(crate) fn control(&self) -> Result<FixtureControl> {
        Ok(FixtureControl {
            uri: self.uri.clone(),
            client: reqwest::blocking::Client::builder().no_proxy().build()?,
        })
    }
}

#[allow(dead_code)] // The standalone fixture binary does not expose runner controls.
impl FixtureControl {
    pub(crate) fn arm_next(&self, action: &str, fault: &str) -> Result<FixtureFaultGuard> {
        let response: ArmNextFaultResponse = self
            .client
            .post(format!("{}/_fixture/faults/next", self.uri))
            .json(&json!({
                "action": action,
                "fault": fault,
            }))
            .send()
            .context("arm fenced catalog next-action fault")?
            .error_for_status()
            .context("fenced catalog next-action fault was rejected")?
            .json()
            .context("decode fenced catalog next-action fault receipt")?;
        Ok(FixtureFaultGuard {
            control: self.clone(),
            arm_id: response.arm_id,
            cleared: false,
        })
    }

    fn clear_arm(&self, arm_id: &str) -> Result<bool> {
        let response: ClearNextFaultResponse = self
            .client
            .delete(format!("{}/_fixture/faults/next/{arm_id}", self.uri))
            .send()
            .context("clear fenced catalog next-action fault")?
            .error_for_status()
            .context("fenced catalog next-action fault cleanup was rejected")?
            .json()
            .context("decode fenced catalog next-action fault cleanup")?;
        Ok(response.entered)
    }
}

#[allow(dead_code)] // The standalone fixture binary does not expose runner controls.
impl FixtureFaultGuard {
    pub(crate) fn finish(mut self) -> Result<()> {
        let entered = self.control.clear_arm(&self.arm_id)?;
        self.cleared = true;
        if !entered {
            anyhow::bail!(
                "fenced catalog next-action fault {} was not consumed by its matching CTAS action",
                self.arm_id
            );
        }
        Ok(())
    }
}

impl Drop for FixtureFaultGuard {
    fn drop(&mut self) {
        if !self.cleared {
            let _ = self.control.clear_arm(&self.arm_id);
            self.cleared = true;
        }
    }
}

impl Drop for FixtureHandle {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[derive(Clone)]
struct AppState {
    downstream: String,
    sqlite_path: PathBuf,
    client: reqwest::Client,
    operation_locks: Arc<Mutex<HashMap<String, Arc<AsyncMutex<()>>>>>,
    faults: Arc<Mutex<HashMap<String, Vec<Fault>>>>,
    next_fault: Arc<Mutex<NextFaultState>>,
    next_fault_sequence: Arc<AtomicU64>,
    delay_entered: Arc<tokio::sync::Notify>,
    cleanup_backend: CleanupBackend,
}

#[derive(Debug, Clone)]
struct ArmedNextFault {
    arm_id: String,
    action: FixtureAction,
    fault: Fault,
}

#[derive(Default)]
struct NextFaultState {
    armed: Option<ArmedNextFault>,
    status: Option<(String, bool)>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum FixtureAction {
    AdvanceFence,
    Stage,
    Publish,
    Abort,
}

#[derive(Clone)]
enum CleanupBackend {
    EnvironmentS3,
    #[cfg(test)]
    Fixed(Operator),
    #[cfg(test)]
    Failing(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
struct Operation {
    cluster_id: String,
    operation_id: String,
    target: TableIdent,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct TableIdent {
    namespace: Vec<String>,
    name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
struct Action {
    operation: Operation,
    generation: Generation,
    action_id: String,
    input_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
struct Generation {
    control_plane_incarnation: u64,
    resource_epoch: u64,
    fence_generation: u64,
}

impl Generation {
    fn is_valid(&self) -> bool {
        self.control_plane_incarnation != 0
            && self.resource_epoch != 0
            && self.fence_generation != 0
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct AdvanceRequest {
    action: Action,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct StageRequest {
    action: Action,
    staged_identity: String,
    initialization_digest: String,
    create_policy: CreatePolicy,
    create_policy_digest: String,
    provider_payload: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct InspectRequest {
    operation: Operation,
    generation: Generation,
    input_digest: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct PublishRequest {
    action: Action,
    staged_locator: String,
    staged_proof: String,
    write_completion_digest: String,
    create_policy: CreatePolicy,
    create_policy_digest: String,
    provider_payload: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct AbortRequest {
    action: Action,
    staged_locator: String,
    staged_proof: String,
    provider_payload: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ActionSeal {
    action_id: String,
    input_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Staged {
    action: ActionSeal,
    identity: String,
    initialization_digest: String,
    create_policy: CreatePolicy,
    create_policy_digest: String,
    provider_payload: String,
    staged_table: Value,
    locator: String,
    proof: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct CleanupDescriptor {
    data_prefixes: Vec<String>,
    objects: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum CreatePolicy {
    FailIfExists,
    NoOpIfExists,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum InFlightKind {
    Stage,
    Publish,
    Abort,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct InFlight {
    kind: InFlightKind,
    action: ActionSeal,
    staged_locator: String,
    staged_proof: String,
    provider_payload: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "kebab-case")]
enum Terminal {
    Published {
        action: ActionSeal,
        provenance: String,
        proof: String,
    },
    NoOp {
        action: ActionSeal,
        provenance: String,
        proof: String,
    },
    Aborted {
        action: ActionSeal,
        provenance: String,
        proof: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct CleanupTerminal {
    action: ActionSeal,
    staged_locator: String,
    staged_proof: String,
    provider_payload_digest: String,
    provenance: String,
    proof: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Record {
    protocol_version: u8,
    operation: Operation,
    generation: Generation,
    fence: ActionSeal,
    #[serde(default)]
    staged_target_identity: Option<String>,
    #[serde(default)]
    current_target_identity: Option<String>,
    staged: Option<Staged>,
    in_flight: Option<InFlight>,
    terminal: Option<Terminal>,
    cleanup_authority: Option<CleanupDescriptor>,
    cleanup_authority_publish_action: Option<ActionSeal>,
    #[serde(default)]
    cleanup: Option<CleanupTerminal>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum Fault {
    BeforeAccept,
    AfterAccept,
    AfterDownstreamBeforeTerminal,
    AfterDownstreamBeforeResponse,
    RecordMissing,
    RecordCorrupt,
    DelayedOldRequest,
}

#[derive(Debug, Deserialize)]
struct FaultRequest {
    fault: Fault,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct ArmNextFaultRequest {
    action: FixtureAction,
    fault: Fault,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[allow(dead_code)] // Decoded only by the in-process SQL runner control client.
struct ArmNextFaultResponse {
    arm_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[allow(dead_code)] // Decoded only by the in-process SQL runner control client.
struct ClearNextFaultResponse {
    entered: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct ReplaceTargetRequest {
    operation: Operation,
    replacement_identity: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestDownstreamAction {
    method: String,
    path: String,
    #[serde(default)]
    body: Value,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum TaggedDownstreamAction {
    #[serde(rename = "iceberg-publish-v1")]
    IcebergPublish {
        action: RestDownstreamAction,
        #[serde(rename = "data-prefixes")]
        data_prefixes: Vec<String>,
        objects: Vec<String>,
    },
    #[serde(rename = "iceberg-cleanup-v1")]
    IcebergCleanup {
        #[serde(rename = "data-prefixes")]
        data_prefixes: Vec<String>,
        objects: Vec<String>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum DownstreamAction {
    Tagged(TaggedDownstreamAction),
    Rest(RestDownstreamAction),
}

#[allow(dead_code)] // The SQL runner uses `FixtureHandle` instead.
pub(crate) async fn serve(config: FixtureConfig) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .with_context(|| format!("bind fenced catalog fixture at {}", config.listen))?;
    let state = build_state(&config)?;
    let app = router(state);
    eprintln!("FENCED_CATALOG_READY listen={}", listener.local_addr()?);
    axum::serve(listener, app)
        .await
        .context("serve fenced catalog fixture")
}

fn build_state(config: &FixtureConfig) -> Result<AppState> {
    initialize_database(&config.sqlite_path)?;
    Ok(AppState {
        downstream: config.downstream.trim_end_matches('/').to_string(),
        sqlite_path: config.sqlite_path.clone(),
        client: reqwest::Client::builder().no_proxy().build()?,
        operation_locks: Arc::new(Mutex::new(HashMap::new())),
        faults: Arc::new(Mutex::new(HashMap::new())),
        next_fault: Arc::new(Mutex::new(NextFaultState::default())),
        next_fault_sequence: Arc::new(AtomicU64::new(1)),
        delay_entered: Arc::new(tokio::sync::Notify::new()),
        cleanup_backend: CleanupBackend::EnvironmentS3,
    })
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/_fixture/faults/{operation_id}", post(arm_fault))
        .route("/_fixture/faults/next", post(arm_next_fault))
        .route("/_fixture/faults/next/{arm_id}", delete(clear_next_fault))
        .route("/_fixture/drop-recreate-target", post(drop_recreate_target))
        .fallback(any(dispatch))
        .with_state(state)
}

fn initialize_database(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let connection = Connection::open(path)?;
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS ctas_operations (\
           operation_key TEXT PRIMARY KEY NOT NULL,\
           record_json TEXT NOT NULL,\
           revision INTEGER NOT NULL DEFAULT 0\
         );",
    )?;
    Ok(())
}

async fn arm_fault(
    State(state): State<AppState>,
    AxumPath(operation_id): AxumPath<String>,
    Json(request): Json<FaultRequest>,
) -> impl IntoResponse {
    state
        .faults
        .lock()
        .expect("fault lock")
        .entry(operation_id)
        .or_default()
        .push(request.fault);
    StatusCode::NO_CONTENT
}

async fn arm_next_fault(
    State(state): State<AppState>,
    Json(request): Json<ArmNextFaultRequest>,
) -> Response {
    let mut next = state.next_fault.lock().expect("next fault lock");
    if next.armed.is_some() || next.status.is_some() {
        return conflict(
            "ambiguous",
            "a bounded next-action fenced catalog fault is already armed",
        );
    }
    let arm_id = format!(
        "next-{}",
        state.next_fault_sequence.fetch_add(1, Ordering::Relaxed)
    );
    next.status = Some((arm_id.clone(), false));
    next.armed = Some(ArmedNextFault {
        arm_id: arm_id.clone(),
        action: request.action,
        fault: request.fault,
    });
    json_response(StatusCode::OK, json!({"arm-id": arm_id}))
}

async fn clear_next_fault(
    State(state): State<AppState>,
    AxumPath(arm_id): AxumPath<String>,
) -> Response {
    let mut next = state.next_fault.lock().expect("next fault lock");
    let entered = match next.status.as_ref() {
        Some((known_arm_id, entered)) if known_arm_id == &arm_id => *entered,
        None => {
            return wire_error(
                StatusCode::NOT_FOUND,
                "identity-conflict",
                "next-action fenced catalog fault token is unknown",
            );
        }
        Some(_) => {
            return wire_error(
                StatusCode::NOT_FOUND,
                "identity-conflict",
                "next-action fenced catalog fault token does not match the active token",
            );
        }
    };
    next.status = None;
    if next.armed.as_ref().is_some_and(|armed| armed.arm_id == arm_id) {
        next.armed = None;
    }
    json_response(StatusCode::OK, json!({"entered": entered}))
}

async fn drop_recreate_target(
    State(state): State<AppState>,
    Json(request): Json<ReplaceTargetRequest>,
) -> Response {
    if request.replacement_identity.trim().is_empty() {
        return wire_error(
            StatusCode::BAD_REQUEST,
            "identity-conflict",
            "replacement target identity must not be empty",
        );
    }
    let key = operation_key(&request.operation);
    match transact(&state, &key, |record| {
        let current =
            record.ok_or_else(|| conflict("ambiguous", "catalog operation record is missing"))?;
        if current.operation != request.operation {
            return Err(conflict("identity-conflict", "operation identity drifted"));
        }
        if current.staged_target_identity.is_none() {
            return Err(conflict(
                "ambiguous",
                "catalog operation has no durable staged target identity",
            ));
        }
        let mut next = current.clone();
        next.current_target_identity = Some(request.replacement_identity.clone());
        Ok(next)
    }) {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(response) => response,
    }
}

async fn dispatch(State(state): State<AppState>, request: Request) -> Response {
    let path = request.uri().path().to_string();
    if request.method() == Method::GET && path.ends_with("/v1/config") {
        return proxy_config(&state, request).await;
    }
    let Some(operation) = extension_operation(&path) else {
        return proxy(&state, request).await;
    };
    let bytes = match axum::body::to_bytes(request.into_body(), MAX_BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(error) => return wire_error(StatusCode::BAD_REQUEST, "ambiguous", error.to_string()),
    };
    match operation {
        "advance-fence" => decode_and_run::<AdvanceRequest>(bytes, &state, handle_advance).await,
        "stage" => decode_and_run::<StageRequest>(bytes, &state, handle_stage).await,
        "inspect" => decode_and_run::<InspectRequest>(bytes, &state, handle_inspect).await,
        "publish" => decode_and_run::<PublishRequest>(bytes, &state, handle_publish).await,
        "abort" => decode_and_run::<AbortRequest>(bytes, &state, handle_abort).await,
        _ => wire_error(
            StatusCode::NOT_IMPLEMENTED,
            "unsupported",
            "unsupported extension operation",
        ),
    }
}

fn extension_operation(path: &str) -> Option<&str> {
    path.split_once("/extensions/fenced-staged-publication/")
        .map(|(_, suffix)| suffix)
}

async fn decode_and_run<T>(
    bytes: Bytes,
    state: &AppState,
    handler: impl AsyncFn(&AppState, T) -> Response,
) -> Response
where
    T: for<'de> Deserialize<'de>,
{
    match serde_json::from_slice(&bytes) {
        Ok(request) => handler(state, request).await,
        Err(error) => wire_error(
            StatusCode::BAD_REQUEST,
            "ambiguous",
            format!("invalid request: {error}"),
        ),
    }
}

trait HasOperation {
    fn operation(&self) -> &Operation;
}
impl HasOperation for AdvanceRequest {
    fn operation(&self) -> &Operation {
        &self.action.operation
    }
}
impl HasOperation for StageRequest {
    fn operation(&self) -> &Operation {
        &self.action.operation
    }
}
impl HasOperation for InspectRequest {
    fn operation(&self) -> &Operation {
        &self.operation
    }
}
impl HasOperation for PublishRequest {
    fn operation(&self) -> &Operation {
        &self.action.operation
    }
}
impl HasOperation for AbortRequest {
    fn operation(&self) -> &Operation {
        &self.action.operation
    }
}

async fn operation_guard<T: HasOperation>(
    state: &AppState,
    request: &T,
) -> tokio::sync::OwnedMutexGuard<()> {
    let key = operation_key(request.operation());
    let lock = {
        let mut locks = state.operation_locks.lock().expect("operation lock map");
        locks
            .entry(key)
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    };
    lock.lock_owned().await
}

async fn apply_delay_fault(state: &AppState, operation_id: &str) {
    if take_fault(state, operation_id, Fault::DelayedOldRequest) {
        state.delay_entered.notify_one();
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn handle_advance(state: &AppState, request: AdvanceRequest) -> Response {
    bind_next_fault(
        state,
        &request.action.operation.operation_id,
        FixtureAction::AdvanceFence,
    );
    apply_delay_fault(state, &request.action.operation.operation_id).await;
    let _guard = operation_guard(state, &request).await;
    if take_fault(
        state,
        &request.action.operation.operation_id,
        Fault::BeforeAccept,
    ) {
        return temporary_failure("advance-fence failed before acceptance");
    }
    if !request.action.generation.is_valid() {
        return conflict(
            "identity-conflict",
            "every ordered generation component must be nonzero",
        );
    }
    let key = operation_key(&request.action.operation);
    let outcome = transact(state, &key, |record| {
        let seal = seal(&request.action);
        Ok(match record {
            None => Record {
                protocol_version: 1,
                operation: request.action.operation.clone(),
                generation: request.action.generation.clone(),
                fence: seal,
                staged_target_identity: None,
                current_target_identity: None,
                staged: None,
                in_flight: None,
                terminal: None,
                cleanup_authority: None,
                cleanup_authority_publish_action: None,
                cleanup: None,
            },
            Some(current) if current.operation != request.action.operation => {
                return Err(conflict(
                    "identity-conflict",
                    "operation target or owner identity drifted",
                ));
            }
            Some(current) if request.action.generation < current.generation => return Err(stale()),
            Some(current) if request.action.generation == current.generation => {
                if current.fence != seal {
                    return Err(conflict(
                        "digest-conflict",
                        "equal generation fence digest drift",
                    ));
                }
                current.clone()
            }
            Some(current) if current.in_flight.is_some() => {
                return Err(conflict(
                    "ambiguous",
                    "higher fence cannot cross an unresolved in-flight catalog action",
                ));
            }
            Some(current) => {
                let mut next = current.clone();
                next.generation = request.action.generation.clone();
                next.fence = seal;
                next
            }
        })
    });
    match outcome {
        Ok(record)
            if take_fault(
                state,
                &request.action.operation.operation_id,
                Fault::AfterAccept,
            ) =>
        {
            temporary_failure("advance-fence response lost after acceptance")
        }
        Ok(record) => json_response(
            StatusCode::OK,
            json!({
                "generation": record.generation,
                "input-digest": record.fence.input_digest,
                "receipt": proof("fence", &key, &generation_string(&record.generation)),
            }),
        ),
        Err(response) => response,
    }
}

async fn handle_stage(state: &AppState, request: StageRequest) -> Response {
    bind_next_fault(
        state,
        &request.action.operation.operation_id,
        FixtureAction::Stage,
    );
    apply_delay_fault(state, &request.action.operation.operation_id).await;
    let _guard = operation_guard(state, &request).await;
    if take_fault(
        state,
        &request.action.operation.operation_id,
        Fault::BeforeAccept,
    ) {
        return temporary_failure("stage failed before acceptance");
    }
    let key = operation_key(&request.action.operation);
    let current = match load_current_for_action(state, &key, &request.action) {
        Ok(record) => record,
        Err(response) => return response,
    };
    if let Some(terminal) = &current.terminal {
        return terminal_conflict(terminal);
    }
    if current.in_flight.is_some() {
        return conflict("ambiguous", "a catalog action remains durably in flight");
    }
    if let Some(staged) = &current.staged {
        let replay_matches = staged.action == seal(&request.action)
            && staged.identity == request.staged_identity
            && staged.initialization_digest == request.initialization_digest
            && staged.create_policy == request.create_policy
            && staged.create_policy_digest == request.create_policy_digest
            && staged.provider_payload == request.provider_payload;
        if !replay_matches {
            return conflict(
                "digest-conflict",
                "stage replay drifted from durable staged identity",
            );
        }
        return json_response(
            StatusCode::OK,
            json!({
                "staged-locator": staged.locator,
                "staged-proof": staged.proof,
                "staged-table": staged.staged_table,
            }),
        );
    }
    let locator = proof("locator", &key, &request.staged_identity);
    let staged_proof = proof("staged", &key, &request.initialization_digest);
    let in_flight = InFlight {
        kind: InFlightKind::Stage,
        action: seal(&request.action),
        staged_locator: locator.clone(),
        staged_proof: staged_proof.clone(),
        provider_payload: request.provider_payload.clone(),
    };
    if let Err(response) = transact(state, &key, |record| {
        let current = require_current(record, &request.action)?;
        if current.in_flight.is_some() {
            return Err(conflict(
                "ambiguous",
                "a catalog action remains durably in flight",
            ));
        }
        let mut next = current.clone();
        next.in_flight = Some(in_flight.clone());
        Ok(next)
    }) {
        return response;
    }
    if take_fault(
        state,
        &request.action.operation.operation_id,
        Fault::AfterAccept,
    ) {
        return temporary_failure("stage failed after durable acceptance");
    }
    let staged_table = match execute_downstream(state, &request.provider_payload).await {
        Ok(DownstreamResult::Success(value)) => value,
        Ok(DownstreamResult::TargetAlreadyExists) => {
            return clear_or_ambiguous(
                state,
                &key,
                &request.action,
                &in_flight,
                conflict(
                    "create-policy-conflict",
                    "downstream target already exists during stage-create",
                ),
            );
        }
        Ok(DownstreamResult::KnownRejection(status)) => {
            return clear_or_ambiguous(
                state,
                &key,
                &request.action,
                &in_flight,
                conflict(
                    "identity-conflict",
                    format!("downstream rejected stage-create with {status}"),
                ),
            );
        }
        Err(DownstreamError::KnownNotDispatched(response)) => {
            return clear_or_ambiguous(state, &key, &request.action, &in_flight, response);
        }
        Err(DownstreamError::OutcomeUnknown(response)) => return response,
    };
    if take_fault(
        state,
        &request.action.operation.operation_id,
        Fault::AfterDownstreamBeforeTerminal,
    ) {
        return temporary_failure("stage crashed after downstream commit before durable record");
    }
    let cleanup_authority =
        match derive_stage_cleanup_authority(&staged_table, &request.action.action_id) {
            Ok(value) => value,
            Err(message) => {
                return temporary_failure(format!(
                    "staged cleanup authority could not be proven: {message}"
                ));
            }
        };
    let target_identity = match staged_target_identity(&staged_table) {
        Ok(value) => value,
        Err(message) => {
            return temporary_failure(format!(
                "staged target identity could not be proven: {message}"
            ));
        }
    };
    let outcome = transact(state, &key, |record| {
        let current = require_current(record, &request.action)?;
        if let Some(terminal) = &current.terminal {
            return Err(terminal_conflict(terminal));
        }
        let staged = Staged {
            action: seal(&request.action),
            identity: request.staged_identity.clone(),
            initialization_digest: request.initialization_digest.clone(),
            create_policy: request.create_policy,
            create_policy_digest: request.create_policy_digest.clone(),
            provider_payload: request.provider_payload.clone(),
            staged_table: staged_table.clone(),
            locator: locator.clone(),
            proof: staged_proof.clone(),
        };
        if current
            .staged
            .as_ref()
            .is_some_and(|value| value != &staged)
        {
            return Err(conflict(
                "digest-conflict",
                "stage replay drifted from durable staged identity",
            ));
        }
        let mut next = current.clone();
        if next.in_flight.as_ref() != Some(&in_flight) {
            return Err(conflict(
                "ambiguous",
                "durable stage intent changed before staged record persistence",
            ));
        }
        next.in_flight = None;
        next.staged = Some(staged);
        match (
            next.staged_target_identity.as_ref(),
            next.current_target_identity.as_ref(),
        ) {
            (None, None) => {
                next.staged_target_identity = Some(target_identity.clone());
                next.current_target_identity = Some(target_identity.clone());
            }
            (Some(staged), Some(current)) if staged == &target_identity && current == staged => {}
            _ => {
                return Err(conflict(
                    "identity-conflict",
                    "staged target identity drifted before durable stage persistence",
                ));
            }
        }
        next.cleanup_authority = Some(cleanup_authority.clone());
        next.cleanup_authority_publish_action = None;
        Ok(next)
    });
    match outcome {
        Ok(record) => {
            if take_fault(
                state,
                &request.action.operation.operation_id,
                Fault::AfterDownstreamBeforeResponse,
            ) {
                return temporary_failure("stage response lost after durable staged result");
            }
            let staged = record.staged.expect("stage persisted");
            json_response(
                StatusCode::OK,
                json!({
                    "staged-locator": staged.locator,
                    "staged-proof": staged.proof,
                    "staged-table": staged.staged_table,
                }),
            )
        }
        Err(response) => response,
    }
}

async fn handle_inspect(state: &AppState, request: InspectRequest) -> Response {
    let _guard = operation_guard(state, &request).await;
    let key = operation_key(&request.operation);
    if take_fault(state, &request.operation.operation_id, Fault::RecordMissing) {
        if let Ok(connection) = Connection::open(&state.sqlite_path) {
            let _ =
                connection.execute("DELETE FROM ctas_operations WHERE operation_key=?1", [&key]);
        }
    }
    if take_fault(state, &request.operation.operation_id, Fault::RecordCorrupt) {
        if let Ok(connection) = Connection::open(&state.sqlite_path) {
            let _ = connection.execute(
                "UPDATE ctas_operations SET record_json='{' WHERE operation_key=?1",
                [&key],
            );
        }
    }
    let record = match load_record(state, &key) {
        Ok(Some(record)) => record,
        Ok(None) => {
            return json_response(
                StatusCode::OK,
                json!({
                    "state":"ambiguous",
                    "message":"catalog operation record is missing",
                    "proof":proof("missing", &key, "record")
                }),
            );
        }
        Err(_) => {
            return json_response(
                StatusCode::OK,
                json!({
                    "state":"ambiguous",
                    "message":"catalog operation record is corrupt",
                    "proof":proof("corrupt", &key, "record")
                }),
            );
        }
    };
    if record.protocol_version != 1 {
        return json_response(
            StatusCode::OK,
            json!({
                "state":"unsupported",
                "message":format!("unsupported durable CTAS fixture protocol version {}", record.protocol_version)
            }),
        );
    }
    if record.operation != request.operation {
        return json_response(
            StatusCode::OK,
            json!({
                "state":"conflict",
                "kind":"identity-conflict",
                "message":"operation target or owner identity drifted",
                "proof":proof("inspect-identity-conflict", &key, &serde_json::to_string(&request.operation).expect("operation serializes"))
            }),
        );
    }
    if let (Some(staged), Some(current)) = (
        record.staged_target_identity.as_deref(),
        record.current_target_identity.as_deref(),
    ) && staged != current
    {
        return json_response(
            StatusCode::OK,
            json!({
                "state":"conflict",
                "kind":"identity-conflict",
                "message":"durable target was dropped and recreated with a new identity",
                "proof":proof("inspect-target-replaced", staged, current)
            }),
        );
    }
    if request.generation != record.generation {
        return json_response(
            StatusCode::OK,
            json!({
                "state":"conflict",
                "kind":"stale-fence",
                "message":"inspection generation is not latest",
                "proof":proof("inspect-stale-fence", &generation_string(&record.generation), &generation_string(&request.generation))
            }),
        );
    }
    if request.input_digest != record.fence.input_digest {
        return json_response(
            StatusCode::OK,
            json!({
                "state":"conflict",
                "kind":"digest-conflict",
                "message":"inspection lineage digest drifted",
                "proof":proof("inspect-digest-conflict", &record.fence.input_digest, &request.input_digest)
            }),
        );
    }
    if record.in_flight.is_some() {
        return json_response(
            StatusCode::OK,
            json!({
                "state":"ambiguous",
                "message":"catalog action remains durably in flight",
                "proof":proof("in-flight", &key, &generation_string(&record.generation))
            }),
        );
    }
    match record.terminal {
        Some(Terminal::Published {
            provenance, proof, ..
        }) => json_response(
            StatusCode::OK,
            json!({"state":"published", "provenance":provenance, "proof":proof}),
        ),
        Some(Terminal::NoOp {
            provenance, proof, ..
        }) => {
            let mut response = json!({
                "state":"no-op",
                "provenance":provenance,
                "proof":proof
            });
            if let Some(staged) = &record.staged {
                response["staged-locator"] = Value::String(staged.locator.clone());
                response["staged-proof"] = Value::String(staged.proof.clone());
            }
            json_response(StatusCode::OK, response)
        }
        Some(Terminal::Aborted {
            provenance, proof, ..
        }) => json_response(
            StatusCode::OK,
            json!({"state":"aborted", "provenance":provenance, "proof":proof}),
        ),
        None if record.staged.is_some() => {
            let staged = record.staged.expect("checked");
            json_response(
                StatusCode::OK,
                json!({"state":"staged", "staged-locator": staged.locator, "proof": staged.proof}),
            )
        }
        None => json_response(
            StatusCode::OK,
            json!({"state":"not-created", "proof": proof("not-created", &key, &generation_string(&record.generation))}),
        ),
    }
}

async fn handle_publish(state: &AppState, request: PublishRequest) -> Response {
    bind_next_fault(
        state,
        &request.action.operation.operation_id,
        FixtureAction::Publish,
    );
    apply_delay_fault(state, &request.action.operation.operation_id).await;
    let _guard = operation_guard(state, &request).await;
    if take_fault(
        state,
        &request.action.operation.operation_id,
        Fault::BeforeAccept,
    ) {
        return temporary_failure("publish failed before acceptance");
    }
    let key = operation_key(&request.action.operation);
    let current = match load_current_for_action(state, &key, &request.action) {
        Ok(record) => record,
        Err(response) => return response,
    };
    let durable_cleanup = match &current.cleanup_authority {
        Some(value) => value.clone(),
        None => {
            return conflict(
                "ambiguous",
                "staged operation has no durable cleanup authority",
            );
        }
    };
    let (publish_action, publish_cleanup) =
        match parse_publish_action(&request.provider_payload, &durable_cleanup) {
            Ok(value) => value,
            Err(response) => return response,
        };
    if current.cleanup_authority_publish_action.is_some()
        && (current.cleanup_authority_publish_action.as_ref() != Some(&seal(&request.action))
            || publish_cleanup != durable_cleanup)
    {
        return conflict(
            "digest-conflict",
            "publish cleanup authority drifted from its durable action binding",
        );
    }
    if let Some(terminal) = &current.terminal {
        return match terminal {
            Terminal::Published {
                action,
                provenance,
                proof,
            } if action == &seal(&request.action) => json_response(
                StatusCode::OK,
                json!({"disposition":"published", "provenance":provenance, "proof":proof}),
            ),
            Terminal::NoOp {
                action,
                provenance,
                proof,
            } if action == &seal(&request.action) => json_response(
                StatusCode::OK,
                json!({"disposition":"no-op", "provenance":provenance, "proof":proof}),
            ),
            other => terminal_conflict(other),
        };
    }
    let staged = match validate_staged(
        &current,
        &request.staged_locator,
        &request.staged_proof,
        Some(request.create_policy),
        Some(&request.create_policy_digest),
    ) {
        Ok(value) => value.clone(),
        Err(response) => return response,
    };
    let in_flight = InFlight {
        kind: InFlightKind::Publish,
        action: seal(&request.action),
        staged_locator: request.staged_locator.clone(),
        staged_proof: request.staged_proof.clone(),
        provider_payload: request.provider_payload.clone(),
    };
    if let Err(response) = transact(state, &key, |record| {
        let current = require_current(record, &request.action)?;
        validate_staged(
            current,
            &request.staged_locator,
            &request.staged_proof,
            Some(request.create_policy),
            Some(&request.create_policy_digest),
        )?;
        if let Some(existing) = &current.in_flight {
            if existing == &in_flight {
                return Err(conflict(
                    "ambiguous",
                    "matching publish remains durably in flight",
                ));
            }
            return Err(conflict(
                "ambiguous",
                "another catalog action remains durably in flight",
            ));
        }
        let mut next = current.clone();
        if next.cleanup_authority_publish_action.is_some()
            && (next.cleanup_authority_publish_action.as_ref() != Some(&seal(&request.action))
                || next.cleanup_authority.as_ref() != Some(&publish_cleanup))
        {
            return Err(conflict(
                "digest-conflict",
                "publish cleanup authority drifted from its durable action binding",
            ));
        }
        if next
            .cleanup_authority
            .as_ref()
            .is_some_and(|value| value.data_prefixes != publish_cleanup.data_prefixes)
        {
            return Err(conflict(
                "digest-conflict",
                "publish cleanup prefixes drifted from durable stage authority",
            ));
        }
        next.cleanup_authority = Some(publish_cleanup.clone());
        next.cleanup_authority_publish_action = Some(seal(&request.action));
        next.in_flight = Some(in_flight.clone());
        Ok(next)
    }) {
        return response;
    }
    if take_fault(
        state,
        &request.action.operation.operation_id,
        Fault::AfterAccept,
    ) {
        return temporary_failure("publish failed after acceptance before downstream commit");
    }
    let downstream = match execute_rest_downstream(state, publish_action).await {
        Ok(outcome) => outcome,
        Err(DownstreamError::KnownNotDispatched(response)) => {
            return clear_or_ambiguous(state, &key, &request.action, &in_flight, response);
        }
        Err(DownstreamError::OutcomeUnknown(response)) => return response,
    };
    if take_fault(
        state,
        &request.action.operation.operation_id,
        Fault::AfterDownstreamBeforeTerminal,
    ) {
        return temporary_failure(
            "publish crashed after downstream commit before durable terminal",
        );
    }
    let no_op = matches!(downstream, DownstreamResult::TargetAlreadyExists)
        && request.create_policy == CreatePolicy::NoOpIfExists;
    if matches!(downstream, DownstreamResult::TargetAlreadyExists) && !no_op {
        return clear_or_ambiguous(
            state,
            &key,
            &request.action,
            &in_flight,
            conflict("create-policy-conflict", "downstream target already exists"),
        );
    }
    if let DownstreamResult::KnownRejection(status) = downstream {
        return clear_or_ambiguous(
            state,
            &key,
            &request.action,
            &in_flight,
            conflict(
                "identity-conflict",
                format!("downstream rejected catalog commit with {status}"),
            ),
        );
    }
    let provenance = proof(
        if no_op { "no-op" } else { "published" },
        &key,
        &request.write_completion_digest,
    );
    let terminal_proof = proof(
        "publish-proof",
        &staged.proof,
        &request.write_completion_digest,
    );
    let terminal = if no_op {
        Terminal::NoOp {
            action: seal(&request.action),
            provenance: provenance.clone(),
            proof: terminal_proof.clone(),
        }
    } else {
        Terminal::Published {
            action: seal(&request.action),
            provenance: provenance.clone(),
            proof: terminal_proof.clone(),
        }
    };
    if let Err(response) = transact(state, &key, |record| {
        let current = require_current(record, &request.action)?;
        validate_staged(
            current,
            &request.staged_locator,
            &request.staged_proof,
            Some(request.create_policy),
            Some(&request.create_policy_digest),
        )?;
        if current.in_flight.as_ref() != Some(&in_flight) {
            return Err(conflict(
                "ambiguous",
                "durable publish intent changed before terminal persistence",
            ));
        }
        if let Some(existing) = &current.terminal {
            if existing != &terminal {
                return Err(terminal_conflict(existing));
            }
        }
        let mut next = current.clone();
        next.in_flight = None;
        next.terminal = Some(terminal.clone());
        Ok(next)
    }) {
        return response;
    }
    if take_fault(
        state,
        &request.action.operation.operation_id,
        Fault::AfterDownstreamBeforeResponse,
    ) {
        return temporary_failure("publish response lost after downstream commit");
    }
    json_response(
        StatusCode::OK,
        json!({"disposition":if no_op {"no-op"} else {"published"}, "provenance":provenance, "proof":terminal_proof}),
    )
}

async fn handle_abort(state: &AppState, request: AbortRequest) -> Response {
    bind_next_fault(
        state,
        &request.action.operation.operation_id,
        FixtureAction::Abort,
    );
    apply_delay_fault(state, &request.action.operation.operation_id).await;
    let _guard = operation_guard(state, &request).await;
    if take_fault(
        state,
        &request.action.operation.operation_id,
        Fault::BeforeAccept,
    ) {
        return temporary_failure("abort failed before acceptance");
    }
    let key = operation_key(&request.action.operation);
    let current = match load_current_for_action(state, &key, &request.action) {
        Ok(record) => record,
        Err(response) => return response,
    };
    let durable_cleanup = match &current.cleanup_authority {
        Some(value) => value.clone(),
        None => {
            return conflict(
                "ambiguous",
                "staged operation has no durable cleanup authority",
            );
        }
    };
    let effective_cleanup_payload =
        match resolve_abort_payload(&request.provider_payload, &durable_cleanup) {
            Ok(value) => value,
            Err(response) => return response,
        };
    let no_op_cleanup = if let Some(terminal) = &current.terminal {
        match terminal {
            Terminal::Aborted {
                action,
                provenance,
                proof,
            } if action == &seal(&request.action) => {
                return json_response(
                    StatusCode::OK,
                    json!({"provenance":provenance, "proof":proof}),
                );
            }
            Terminal::NoOp { .. } => {
                if let Some(cleanup) = &current.cleanup {
                    if cleanup.action == seal(&request.action)
                        && cleanup.staged_locator == request.staged_locator
                        && cleanup.staged_proof == request.staged_proof
                        && cleanup.provider_payload_digest == digest(&effective_cleanup_payload)
                    {
                        return json_response(
                            StatusCode::OK,
                            json!({"provenance":cleanup.provenance, "proof":cleanup.proof}),
                        );
                    }
                    return conflict(
                        "digest-conflict",
                        "no-op staging cleanup action identity drifted",
                    );
                }
                if current.staged.is_none() {
                    return conflict(
                        "already-published",
                        "no-op operation has no retained staging to clean",
                    );
                }
                true
            }
            other => return terminal_conflict(other),
        }
    } else {
        false
    };
    let staged = match validate_staged(
        &current,
        &request.staged_locator,
        &request.staged_proof,
        None,
        None,
    ) {
        Ok(value) => value.clone(),
        Err(response) => return response,
    };
    let in_flight = InFlight {
        kind: InFlightKind::Abort,
        action: seal(&request.action),
        staged_locator: request.staged_locator.clone(),
        staged_proof: request.staged_proof.clone(),
        provider_payload: effective_cleanup_payload.clone(),
    };
    if let Err(response) = transact(state, &key, |record| {
        let current = require_current(record, &request.action)?;
        if current.cleanup_authority.as_ref() != Some(&durable_cleanup) {
            return Err(conflict(
                "digest-conflict",
                "cleanup authority changed before durable abort acceptance",
            ));
        }
        validate_staged(
            current,
            &request.staged_locator,
            &request.staged_proof,
            None,
            None,
        )?;
        if let Some(existing) = &current.in_flight {
            if existing == &in_flight {
                return Err(conflict(
                    "ambiguous",
                    "matching abort remains durably in flight",
                ));
            }
            return Err(conflict(
                "ambiguous",
                "another catalog action remains durably in flight",
            ));
        }
        let mut next = current.clone();
        next.in_flight = Some(in_flight.clone());
        Ok(next)
    }) {
        return response;
    }
    if take_fault(
        state,
        &request.action.operation.operation_id,
        Fault::AfterAccept,
    ) {
        return temporary_failure("abort failed after acceptance before downstream commit");
    }
    let downstream = match execute_downstream(state, &effective_cleanup_payload).await {
        Ok(outcome) => outcome,
        Err(DownstreamError::KnownNotDispatched(response)) => {
            return clear_or_ambiguous(state, &key, &request.action, &in_flight, response);
        }
        Err(DownstreamError::OutcomeUnknown(response)) => return response,
    };
    if take_fault(
        state,
        &request.action.operation.operation_id,
        Fault::AfterDownstreamBeforeTerminal,
    ) {
        return temporary_failure("abort crashed after downstream commit before durable terminal");
    }
    if let DownstreamResult::TargetAlreadyExists = downstream {
        return clear_or_ambiguous(
            state,
            &key,
            &request.action,
            &in_flight,
            conflict(
                "identity-conflict",
                "downstream rejected guarded abort because target already exists",
            ),
        );
    }
    if let DownstreamResult::KnownRejection(status) = downstream {
        return clear_or_ambiguous(
            state,
            &key,
            &request.action,
            &in_flight,
            conflict(
                "identity-conflict",
                format!("downstream rejected guarded abort with {status}"),
            ),
        );
    }
    let provenance = proof("aborted", &key, &staged.identity);
    let terminal_proof = proof("abort-proof", &staged.proof, &request.action.input_digest);
    let terminal = (!no_op_cleanup).then(|| Terminal::Aborted {
        action: seal(&request.action),
        provenance: provenance.clone(),
        proof: terminal_proof.clone(),
    });
    let cleanup = no_op_cleanup.then(|| CleanupTerminal {
        action: seal(&request.action),
        staged_locator: request.staged_locator.clone(),
        staged_proof: request.staged_proof.clone(),
        provider_payload_digest: digest(&effective_cleanup_payload),
        provenance: provenance.clone(),
        proof: terminal_proof.clone(),
    });
    if let Err(response) = transact(state, &key, |record| {
        let current = require_current(record, &request.action)?;
        validate_staged(
            current,
            &request.staged_locator,
            &request.staged_proof,
            None,
            None,
        )?;
        if current.in_flight.as_ref() != Some(&in_flight) {
            return Err(conflict(
                "ambiguous",
                "durable abort intent changed before terminal persistence",
            ));
        }
        let mut next = current.clone();
        next.in_flight = None;
        if no_op_cleanup {
            if !matches!(current.terminal, Some(Terminal::NoOp { .. })) {
                return Err(conflict(
                    "already-published",
                    "only no-op terminal may retain proof-bound staging cleanup",
                ));
            }
            next.cleanup = cleanup.clone();
            next.staged = None;
        } else {
            if let Some(existing) = &current.terminal {
                if Some(existing) != terminal.as_ref() {
                    return Err(terminal_conflict(existing));
                }
            }
            next.terminal = terminal.clone();
        }
        Ok(next)
    }) {
        return response;
    }
    if take_fault(
        state,
        &request.action.operation.operation_id,
        Fault::AfterDownstreamBeforeResponse,
    ) {
        return temporary_failure("abort response lost after downstream commit");
    }
    json_response(
        StatusCode::OK,
        json!({"provenance":provenance, "proof":terminal_proof}),
    )
}

enum DownstreamResult {
    Success(Value),
    TargetAlreadyExists,
    KnownRejection(StatusCode),
}

enum DownstreamError {
    KnownNotDispatched(Response),
    OutcomeUnknown(Response),
}

async fn execute_downstream(
    state: &AppState,
    payload: &str,
) -> std::result::Result<DownstreamResult, DownstreamError> {
    if payload.trim().is_empty() {
        return Ok(DownstreamResult::Success(Value::Null));
    }
    let action: DownstreamAction = serde_json::from_str(payload).map_err(|error| {
        DownstreamError::KnownNotDispatched(wire_error(
            StatusCode::CONFLICT,
            "ambiguous",
            format!("invalid fixture downstream action: {error}"),
        ))
    })?;
    let action = match action {
        DownstreamAction::Rest(action) => action,
        DownstreamAction::Tagged(TaggedDownstreamAction::IcebergPublish { action, .. }) => action,
        DownstreamAction::Tagged(TaggedDownstreamAction::IcebergCleanup {
            data_prefixes,
            objects,
        }) => {
            validate_cleanup_payload(&data_prefixes, &objects).map_err(|message| {
                DownstreamError::KnownNotDispatched(conflict("identity-conflict", message))
            })?;
            return execute_cleanup(state, data_prefixes, objects)
                .await
                .map(|()| DownstreamResult::Success(Value::Null))
                .map_err(|message| {
                    DownstreamError::KnownNotDispatched(conflict("identity-conflict", message))
                });
        }
    };
    execute_rest_downstream(state, action).await
}

async fn execute_rest_downstream(
    state: &AppState,
    action: RestDownstreamAction,
) -> std::result::Result<DownstreamResult, DownstreamError> {
    let method = Method::from_bytes(action.method.as_bytes()).map_err(|error| {
        DownstreamError::KnownNotDispatched(wire_error(
            StatusCode::CONFLICT,
            "ambiguous",
            error.to_string(),
        ))
    })?;
    if !action.path.starts_with('/') {
        return Err(DownstreamError::KnownNotDispatched(wire_error(
            StatusCode::CONFLICT,
            "ambiguous",
            "downstream path must be absolute",
        )));
    }
    let response = state
        .client
        .request(method, format!("{}{}", state.downstream, action.path))
        .json(&action.body)
        .send()
        .await
        .map_err(|error| {
            DownstreamError::OutcomeUnknown(temporary_failure(format!(
                "downstream outcome unknown: {error}"
            )))
        })?;
    if response.status().is_success() {
        let bytes = response.bytes().await.map_err(|error| {
            DownstreamError::OutcomeUnknown(temporary_failure(format!(
                "downstream response outcome unknown: {error}"
            )))
        })?;
        if bytes.len() > MAX_BODY_BYTES {
            return Err(DownstreamError::OutcomeUnknown(temporary_failure(
                "downstream success body exceeds the bounded wire limit",
            )));
        }
        if bytes.is_empty() {
            Ok(DownstreamResult::Success(Value::Null))
        } else {
            serde_json::from_slice(&bytes)
                .map(DownstreamResult::Success)
                .map_err(|error| {
                    DownstreamError::OutcomeUnknown(temporary_failure(format!(
                        "downstream REST success body is invalid: {error}"
                    )))
                })
        }
    } else if response.status() == StatusCode::CONFLICT {
        let bytes = response.bytes().await.map_err(|error| {
            DownstreamError::OutcomeUnknown(temporary_failure(format!(
                "downstream rejection body outcome unknown: {error}"
            )))
        })?;
        if bytes.len() > MAX_BODY_BYTES {
            return Err(DownstreamError::OutcomeUnknown(temporary_failure(
                "downstream rejection body exceeds the bounded wire limit",
            )));
        }
        if is_target_already_exists(&bytes) {
            Ok(DownstreamResult::TargetAlreadyExists)
        } else {
            Ok(DownstreamResult::KnownRejection(StatusCode::CONFLICT))
        }
    } else if response.status().is_client_error() {
        Ok(DownstreamResult::KnownRejection(response.status()))
    } else {
        Err(DownstreamError::OutcomeUnknown(temporary_failure(format!(
            "downstream server outcome unknown with status {}",
            response.status()
        ))))
    }
}

fn is_target_already_exists(bytes: &[u8]) -> bool {
    let Ok(value) = serde_json::from_slice::<Value>(bytes) else {
        return false;
    };
    value
        .pointer("/error/type")
        .and_then(Value::as_str)
        .is_some_and(|value| value == "AlreadyExistsException")
        && value
            .pointer("/error/code")
            .and_then(Value::as_u64)
            .is_some_and(|value| value == 409)
}

fn derive_stage_cleanup_authority(
    staged_table: &Value,
    stage_action_id: &str,
) -> std::result::Result<CleanupDescriptor, String> {
    if stage_action_id.is_empty()
        || stage_action_id.len() > MAX_CLEANUP_PATH_BYTES
        || !stage_action_id
            .chars()
            .all(|value| value.is_ascii_alphanumeric() || value == '-' || value == '_')
    {
        return Err("stage action id is not safe for an Iceberg staging prefix".to_string());
    }
    let table_location = staged_table
        .get("metadata")
        .and_then(|metadata| metadata.get("location"))
        .and_then(Value::as_str)
        .ok_or_else(|| "staged table metadata is missing its table location".to_string())?
        .trim_end_matches('/');
    if table_location.is_empty() {
        return Err("staged table metadata has an empty table location".to_string());
    }
    let descriptor = CleanupDescriptor {
        data_prefixes: vec![format!("{table_location}/data/_staging/{stage_action_id}/")],
        objects: Vec::new(),
    };
    validate_cleanup_payload(&descriptor.data_prefixes, &descriptor.objects)?;
    Ok(descriptor)
}

fn staged_target_identity(staged_table: &Value) -> std::result::Result<String, String> {
    let identity = staged_table
        .get("metadata")
        .and_then(|metadata| metadata.get("table-uuid"))
        .and_then(Value::as_str)
        .ok_or_else(|| "staged table metadata is missing its table UUID".to_string())?;
    if identity.trim().is_empty() || identity.len() > MAX_CLEANUP_PATH_BYTES {
        return Err("staged table UUID is empty or exceeds the bounded identity limit".to_string());
    }
    Ok(identity.to_string())
}

fn parse_publish_action(
    payload: &str,
    durable: &CleanupDescriptor,
) -> std::result::Result<(RestDownstreamAction, CleanupDescriptor), Response> {
    let action: DownstreamAction = serde_json::from_str(payload).map_err(|error| {
        conflict(
            "identity-conflict",
            format!("invalid fixture publish action: {error}"),
        )
    })?;
    match action {
        DownstreamAction::Rest(_) => Err(conflict(
            "identity-conflict",
            "publish requires iceberg-publish-v1 cleanup authority",
        )),
        DownstreamAction::Tagged(TaggedDownstreamAction::IcebergPublish {
            action,
            data_prefixes,
            objects,
        }) => {
            validate_cleanup_payload(&data_prefixes, &objects)
                .map_err(|message| conflict("identity-conflict", message))?;
            if data_prefixes != durable.data_prefixes {
                return Err(conflict(
                    "digest-conflict",
                    "publish cleanup prefixes drifted from durable stage authority",
                ));
            }
            Ok((
                action,
                CleanupDescriptor {
                    data_prefixes,
                    objects,
                },
            ))
        }
        DownstreamAction::Tagged(TaggedDownstreamAction::IcebergCleanup { .. }) => Err(conflict(
            "identity-conflict",
            "publish requires an iceberg-publish-v1 payload",
        )),
    }
}

fn cleanup_payload(descriptor: &CleanupDescriptor) -> String {
    json!({
        "kind":"iceberg-cleanup-v1",
        "data-prefixes":descriptor.data_prefixes,
        "objects":descriptor.objects
    })
    .to_string()
}

fn resolve_abort_payload(
    payload: &str,
    durable: &CleanupDescriptor,
) -> std::result::Result<String, Response> {
    if payload.trim().is_empty() {
        return Ok(cleanup_payload(durable));
    }
    let action: DownstreamAction = serde_json::from_str(payload).map_err(|error| {
        conflict(
            "identity-conflict",
            format!("invalid fixture cleanup action: {error}"),
        )
    })?;
    let supplied = match action {
        DownstreamAction::Tagged(TaggedDownstreamAction::IcebergCleanup {
            data_prefixes,
            objects,
        }) => CleanupDescriptor {
            data_prefixes,
            objects,
        },
        _ => {
            return Err(conflict(
                "identity-conflict",
                "abort provider payload must be empty or iceberg-cleanup-v1",
            ));
        }
    };
    validate_cleanup_payload(&supplied.data_prefixes, &supplied.objects)
        .map_err(|message| conflict("identity-conflict", message))?;
    if &supplied != durable {
        return Err(conflict(
            "digest-conflict",
            "cleanup payload drifted from durable catalog authority",
        ));
    }
    Ok(cleanup_payload(&supplied))
}

fn validate_cleanup_payload(
    prefixes: &[String],
    objects: &[String],
) -> std::result::Result<(), String> {
    let count = prefixes.len().saturating_add(objects.len());
    if count == 0 || count > MAX_CLEANUP_ITEMS {
        return Err(format!(
            "iceberg cleanup item count must be in 1..={MAX_CLEANUP_ITEMS}"
        ));
    }
    let mut total = 0usize;
    for path in prefixes.iter().chain(objects) {
        if path.is_empty() || path.len() > MAX_CLEANUP_PATH_BYTES {
            return Err(format!(
                "iceberg cleanup path must be in 1..={MAX_CLEANUP_PATH_BYTES} bytes"
            ));
        }
        total = total.saturating_add(path.len());
        if total > MAX_CLEANUP_TOTAL_PATH_BYTES {
            return Err(format!(
                "iceberg cleanup paths exceed {MAX_CLEANUP_TOTAL_PATH_BYTES} total bytes"
            ));
        }
        let raw = path.strip_prefix("s3://").unwrap_or(path);
        if path.starts_with('/')
            || raw.trim_matches('/').is_empty()
            || raw
                .chars()
                .any(|value| matches!(value, '@' | '?' | '#' | '\\' | '\0'))
            || raw
                .split('/')
                .any(|segment| segment == "." || segment == "..")
        {
            return Err(
                "iceberg cleanup paths must not contain credentials or traversal".to_string(),
            );
        }
        if path.contains("://") && !path.starts_with("s3://") {
            return Err("iceberg cleanup paths must be relative or use s3://".to_string());
        }
    }
    Ok(())
}

async fn execute_cleanup(
    state: &AppState,
    prefixes: Vec<String>,
    objects: Vec<String>,
) -> std::result::Result<(), String> {
    for prefix in prefixes {
        let (operator, path) = cleanup_operator(state, &prefix)?;
        operator
            .remove_all(&path)
            .await
            .map_err(|error| format!("remove cleanup prefix failed: {error}"))?;
    }
    for object in objects {
        let (operator, path) = cleanup_operator(state, &object)?;
        operator
            .delete(&path)
            .await
            .map_err(|error| format!("delete cleanup object failed: {error}"))?;
    }
    Ok(())
}

fn cleanup_operator(
    state: &AppState,
    location: &str,
) -> std::result::Result<(Operator, String), String> {
    match &state.cleanup_backend {
        #[cfg(test)]
        CleanupBackend::Fixed(operator) => {
            let path = location
                .strip_prefix("s3://")
                .and_then(|raw| raw.split_once('/').map(|(_, path)| path))
                .unwrap_or(location);
            Ok((operator.clone(), path.to_string()))
        }
        CleanupBackend::EnvironmentS3 => {
            let raw = location
                .strip_prefix("s3://")
                .ok_or_else(|| "S3 cleanup requires an s3:// location".to_string())?;
            let (bucket, path) = raw
                .split_once('/')
                .ok_or_else(|| "S3 cleanup location must include bucket and path".to_string())?;
            if bucket.is_empty() || path.is_empty() {
                return Err("S3 cleanup location must include bucket and path".to_string());
            }
            let endpoint = env::var("AWS_S3_ENDPOINT")
                .map_err(|_| "AWS_S3_ENDPOINT is required for iceberg cleanup".to_string())?;
            let access_key = env::var("AWS_S3_ACCESS_KEY_ID")
                .map_err(|_| "AWS_S3_ACCESS_KEY_ID is required for iceberg cleanup".to_string())?;
            let secret_key = env::var("AWS_S3_SECRET_ACCESS_KEY").map_err(|_| {
                "AWS_S3_SECRET_ACCESS_KEY is required for iceberg cleanup".to_string()
            })?;
            // OpenDAL S3 uses path-style addressing unless virtual-host style is explicitly enabled.
            let operator = Operator::new(
                opendal::services::S3::default()
                    .endpoint(&endpoint)
                    .bucket(bucket)
                    .region("us-east-1")
                    .access_key_id(&access_key)
                    .secret_access_key(&secret_key),
            )
            .map_err(|error| format!("build S3 cleanup operator failed: {error}"))?
            .finish();
            Ok((operator, path.to_string()))
        }
        #[cfg(test)]
        CleanupBackend::Failing(message) => Err(message.clone()),
    }
}

fn validate_staged<'a>(
    record: &'a Record,
    locator: &str,
    proof_value: &str,
    create_policy: Option<CreatePolicy>,
    create_policy_digest: Option<&str>,
) -> std::result::Result<&'a Staged, Response> {
    let staged = record
        .staged
        .as_ref()
        .ok_or_else(|| conflict("identity-conflict", "operation has no staged target"))?;
    if staged.locator != locator || staged.proof != proof_value {
        return Err(conflict(
            "identity-conflict",
            "staged locator or proof does not match",
        ));
    }
    if create_policy.is_some_and(|value| value != staged.create_policy) {
        return Err(conflict(
            "create-policy-conflict",
            "explicit create policy drifted",
        ));
    }
    if create_policy_digest.is_some_and(|value| value != staged.create_policy_digest) {
        return Err(conflict(
            "create-policy-conflict",
            "create policy digest drifted",
        ));
    }
    Ok(staged)
}

fn load_current_for_action(
    state: &AppState,
    key: &str,
    action: &Action,
) -> std::result::Result<Record, Response> {
    match load_record(state, key) {
        Ok(Some(record)) => {
            if record.protocol_version != 1 {
                return Err(unsupported_protocol(record.protocol_version));
            }
            require_current(Some(&record), action)?;
            Ok(record)
        }
        Ok(None) => Err(conflict("ambiguous", "catalog operation record is missing")),
        Err(_) => Err(conflict("ambiguous", "catalog operation record is corrupt")),
    }
}

fn clear_in_flight(
    state: &AppState,
    key: &str,
    action: &Action,
    expected: &InFlight,
) -> std::result::Result<Record, Response> {
    transact(state, key, |record| {
        let current = require_current(record, action)?;
        if current.in_flight.as_ref() != Some(expected) {
            return Err(conflict(
                "ambiguous",
                "durable in-flight action changed before clearing",
            ));
        }
        let mut next = current.clone();
        next.in_flight = None;
        Ok(next)
    })
}

fn clear_or_ambiguous(
    state: &AppState,
    key: &str,
    action: &Action,
    expected: &InFlight,
    known_response: Response,
) -> Response {
    match clear_in_flight(state, key, action, expected) {
        Ok(_) => known_response,
        Err(_) => conflict(
            "ambiguous",
            "known downstream failure could not clear its durable in-flight action",
        ),
    }
}

fn require_current<'a>(
    record: Option<&'a Record>,
    action: &Action,
) -> std::result::Result<&'a Record, Response> {
    let record =
        record.ok_or_else(|| conflict("ambiguous", "catalog operation record is missing"))?;
    if record.operation != action.operation {
        return Err(conflict("identity-conflict", "operation identity drifted"));
    }
    if let (Some(staged), Some(current)) = (
        record.staged_target_identity.as_deref(),
        record.current_target_identity.as_deref(),
    ) && staged != current
    {
        return Err(conflict(
            "identity-conflict",
            "durable target was dropped and recreated with a new identity",
        ));
    }
    if action.generation != record.generation {
        return Err(stale());
    }
    Ok(record)
}

fn transact(
    state: &AppState,
    key: &str,
    update: impl FnOnce(Option<&Record>) -> std::result::Result<Record, Response>,
) -> std::result::Result<Record, Response> {
    let mut connection = Connection::open(&state.sqlite_path).map_err(db_ambiguous)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(db_ambiguous)?;
    let raw: Option<String> = transaction
        .query_row(
            "SELECT record_json FROM ctas_operations WHERE operation_key=?1",
            [key],
            |row| row.get(0),
        )
        .optional()
        .map_err(db_ambiguous)?;
    let current = raw
        .as_deref()
        .map(serde_json::from_str::<Record>)
        .transpose()
        .map_err(|_| conflict("ambiguous", "catalog operation record is corrupt"))?;
    if let Some(current) = &current
        && current.protocol_version != 1
    {
        return Err(unsupported_protocol(current.protocol_version));
    }
    let next = update(current.as_ref())?;
    let encoded = serde_json::to_string(&next).map_err(db_ambiguous)?;
    transaction.execute(
        "INSERT INTO ctas_operations(operation_key,record_json,revision) VALUES(?1,?2,1) ON CONFLICT(operation_key) DO UPDATE SET record_json=excluded.record_json,revision=ctas_operations.revision+1",
        params![key, encoded],
    ).map_err(db_ambiguous)?;
    transaction.commit().map_err(db_ambiguous)?;
    Ok(next)
}

fn load_record(state: &AppState, key: &str) -> Result<Option<Record>> {
    let connection = Connection::open(&state.sqlite_path)?;
    let raw: Option<String> = connection
        .query_row(
            "SELECT record_json FROM ctas_operations WHERE operation_key=?1",
            [key],
            |row| row.get(0),
        )
        .optional()?;
    raw.map(|value| serde_json::from_str(&value).context("decode durable CTAS operation record"))
        .transpose()
}

async fn proxy_config(state: &AppState, request: Request) -> Response {
    let response = proxy_request(state, request).await;
    let (parts, body) = response.into_parts();
    if !parts.status.is_success() {
        return Response::from_parts(parts, body);
    }
    let bytes = match axum::body::to_bytes(body, MAX_BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(error) => return temporary_failure(error.to_string()),
    };
    let mut value: Value = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(error) => return temporary_failure(format!("downstream config is invalid: {error}")),
    };
    let Some(root) = value.as_object_mut() else {
        return temporary_failure("downstream config is not an object");
    };
    let defaults = root.entry("defaults").or_insert_with(|| json!({}));
    let Some(defaults) = defaults.as_object_mut() else {
        return temporary_failure("downstream config defaults is not an object");
    };
    defaults.insert(CAPABILITY.to_string(), Value::String("1".to_string()));
    let overrides = root.entry("overrides").or_insert_with(|| json!({}));
    let Some(overrides) = overrides.as_object_mut() else {
        return temporary_failure("downstream config overrides is not an object");
    };
    overrides.insert(CAPABILITY.to_string(), Value::String("1".to_string()));
    json_response(StatusCode::OK, value)
}

async fn proxy(state: &AppState, request: Request) -> Response {
    proxy_request(state, request).await
}

async fn proxy_request(state: &AppState, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let bytes = match axum::body::to_bytes(body, MAX_PROXY_BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(error) => return temporary_failure(error.to_string()),
    };
    let url = format!(
        "{}{}",
        state.downstream,
        parts
            .uri
            .path_and_query()
            .map(|v| v.as_str())
            .unwrap_or("/")
    );
    let mut outbound = state.client.request(parts.method, url).body(bytes);
    for (name, value) in parts.headers.iter() {
        if name != axum::http::header::HOST && name != axum::http::header::CONTENT_LENGTH {
            outbound = outbound.header(name, value);
        }
    }
    let response = match outbound.send().await {
        Ok(response) => response,
        Err(error) => return temporary_failure(format!("downstream REST request failed: {error}")),
    };
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(error) => return temporary_failure(format!("read downstream REST response: {error}")),
    };
    response_with_headers(status, headers, bytes)
}

fn response_with_headers(status: StatusCode, headers: HeaderMap, bytes: Bytes) -> Response {
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = status;
    for (name, value) in headers.iter() {
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
        json!({"error":{"kind":kind,"message":message.into()}}),
    )
}
fn temporary_failure(message: impl Into<String>) -> Response {
    wire_error(StatusCode::SERVICE_UNAVAILABLE, "ambiguous", message)
}
fn conflict(kind: &str, message: impl Into<String>) -> Response {
    wire_error(StatusCode::CONFLICT, kind, message)
}
fn stale() -> Response {
    wire_error(
        StatusCode::PRECONDITION_FAILED,
        "stale-fence",
        "request generation is not the latest catalog fence",
    )
}
fn db_ambiguous(error: impl std::fmt::Display) -> Response {
    conflict(
        "ambiguous",
        format!("catalog operation database failure: {error}"),
    )
}
fn terminal_conflict(terminal: &Terminal) -> Response {
    match terminal {
        Terminal::Published { .. } => {
            conflict("already-published", "operation is already published")
        }
        Terminal::NoOp { .. } => conflict(
            "already-published",
            "operation already completed as a no-op",
        ),
        Terminal::Aborted { .. } => conflict("already-aborted", "operation is already aborted"),
    }
}

fn unsupported_protocol(version: u8) -> Response {
    wire_error(
        StatusCode::NOT_IMPLEMENTED,
        "unsupported",
        format!("unsupported durable CTAS fixture protocol version {version}"),
    )
}
fn seal(action: &Action) -> ActionSeal {
    ActionSeal {
        action_id: action.action_id.clone(),
        input_digest: action.input_digest.clone(),
    }
}
fn operation_key(operation: &Operation) -> String {
    serde_json::to_string(&(&operation.cluster_id, &operation.operation_id))
        .expect("operation identity serializes")
}
fn generation_string(generation: &Generation) -> String {
    format!(
        "{}:{}:{}",
        generation.control_plane_incarnation,
        generation.resource_epoch,
        generation.fence_generation
    )
}
fn proof(domain: &str, first: &str, second: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain.as_bytes());
    hasher.update([0]);
    hasher.update(first.as_bytes());
    hasher.update([0]);
    hasher.update(second.as_bytes());
    format!("{domain}:{:x}", hasher.finalize())
}

fn digest(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    format!("{:x}", hasher.finalize())
}
fn take_fault(state: &AppState, operation_id: &str, expected: Fault) -> bool {
    let mut faults = state.faults.lock().expect("fault lock");
    let Some(queue) = faults.get_mut(operation_id) else {
        return false;
    };
    let Some(index) = queue.iter().position(|fault| *fault == expected) else {
        return false;
    };
    queue.remove(index);
    true
}

fn bind_next_fault(state: &AppState, operation_id: &str, action: FixtureAction) {
    let armed = {
        let mut next = state.next_fault.lock().expect("next fault lock");
        match next.armed.as_ref() {
            Some(armed) if armed.action == action => {
                let armed = next.armed.take();
                if let (Some(armed), Some((arm_id, entered))) = (armed.as_ref(), next.status.as_mut())
                    && arm_id == &armed.arm_id
                {
                    *entered = true;
                }
                armed
            }
            _ => None,
        }
    };
    let Some(armed) = armed else {
        return;
    };
    state
        .faults
        .lock()
        .expect("fault lock")
        .entry(operation_id.to_string())
        .or_default()
        .push(armed.fault);
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::{get, post};
    use serde_json::json;
    use tempfile::TempDir;
    use tokio::task::JoinHandle;

    struct RunningServer {
        uri: String,
        task: JoinHandle<()>,
        delay_entered: Option<Arc<tokio::sync::Notify>>,
    }

    impl Drop for RunningServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn start_router(app: Router) -> RunningServer {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        RunningServer {
            uri: format!("http://{address}"),
            task,
            delay_entered: None,
        }
    }

    async fn start_fixture_router(
        app: Router,
        delay_entered: Arc<tokio::sync::Notify>,
    ) -> RunningServer {
        let mut server = start_router(app).await;
        server.delay_entered = Some(delay_entered);
        server
    }

    async fn downstream() -> RunningServer {
        start_router(
            Router::new()
                .route(
                    "/v1/config",
                    get(|| async {
                        Json(json!({"defaults":{"warehouse":"s3://warehouse"},"overrides":{}}))
                    }),
                )
                .route(
                    "/stage",
                    post(|| async {
                        Json(json!({
                            "metadata-location":"s3://warehouse/staged/metadata/00000.metadata.json",
                            "metadata":{
                                "format-version":2,
                                "location":"s3://warehouse/staged",
                                "table-uuid":"00000000-0000-4000-8000-000000000001"
                            },
                            "config":{}
                        }))
                    }),
                )
                .route(
                    "/commit",
                    post(|| async { Json(json!({"committed":true})) }),
                )
                .route(
                    "/already-exists",
                    post(|| async {
                        (
                            StatusCode::CONFLICT,
                            Json(json!({
                                "error":{
                                    "message":"target already exists",
                                    "type":"AlreadyExistsException",
                                    "code":409
                                }
                            })),
                        )
                    }),
                )
                .route(
                    "/conflict",
                    post(|| async {
                        (
                            StatusCode::CONFLICT,
                            Json(json!({"error":{"message":"commit requirement failed","type":"CommitFailedException","code":409}})),
                        )
                    }),
                )
                .route(
                    "/misleading-conflict",
                    post(|| async {
                        (
                            StatusCode::CONFLICT,
                            Json(json!({
                                "error":{
                                    "message":"target already exists",
                                    "code":409
                                }
                            })),
                        )
                    }),
                )
                .route(
                    "/v1/namespaces",
                    get(|| async { Json(json!({"namespaces":[["db"]]})) }),
                ),
        )
        .await
    }

    async fn fixture(downstream: &str, sqlite_path: &Path) -> RunningServer {
        let config = FixtureConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            downstream: downstream.to_string(),
            sqlite_path: sqlite_path.to_path_buf(),
        };
        let state = build_state(&config).unwrap();
        let delay_entered = state.delay_entered.clone();
        start_fixture_router(router(state), delay_entered).await
    }

    async fn fixture_with_cleanup_backend(
        downstream: &str,
        sqlite_path: &Path,
        cleanup_backend: CleanupBackend,
    ) -> RunningServer {
        let config = FixtureConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            downstream: downstream.to_string(),
            sqlite_path: sqlite_path.to_path_buf(),
        };
        let mut state = build_state(&config).unwrap();
        state.cleanup_backend = cleanup_backend;
        let delay_entered = state.delay_entered.clone();
        start_fixture_router(router(state), delay_entered).await
    }

    fn generation(value: u64) -> Value {
        json!({
            "control-plane-incarnation":1,
            "resource-epoch":1,
            "fence-generation":value
        })
    }

    fn operation() -> Value {
        json!({
            "cluster-id":"cluster-a",
            "operation-id":"operation-a",
            "target":{"namespace":["db"],"name":"target"}
        })
    }

    fn action(generation_value: u64, action_id: &str, digest: &str) -> Value {
        json!({
            "operation":operation(),
            "generation":generation(generation_value),
            "action-id":action_id,
            "input-digest":digest
        })
    }

    fn downstream_payload(path: &str) -> String {
        json!({"method":"POST","path":path,"body":{"stage-create":true}}).to_string()
    }

    fn publish_payload(path: &str, objects: Vec<&str>) -> String {
        json!({
            "kind":"iceberg-publish-v1",
            "action":{"method":"POST","path":path,"body":{"stage-create":true}},
            "data-prefixes":["s3://warehouse/staged/data/_staging/stage/"],
            "objects":objects
        })
        .to_string()
    }

    async fn post_json(
        client: &reqwest::Client,
        uri: &str,
        operation_name: &str,
        body: Value,
    ) -> reqwest::Response {
        client
            .post(format!(
                "{uri}/v1/extensions/fenced-staged-publication/{operation_name}"
            ))
            .json(&body)
            .send()
            .await
            .unwrap()
    }

    async fn inject_fault(client: &reqwest::Client, uri: &str, fault: &str) {
        let response = client
            .post(format!("{uri}/_fixture/faults/operation-a"))
            .json(&json!({"fault":fault}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    fn stage_request() -> Value {
        json!({
            "action":action(1,"stage","stage-digest"),
            "staged-identity":"staged-a",
            "initialization-digest":"init-a",
            "create-policy":"fail-if-exists",
            "create-policy-digest":"policy-a",
            "provider-payload":downstream_payload("/stage")
        })
    }

    fn abort_request(staged: &Value) -> Value {
        json!({
            "action":action(1,"abort","abort-a"),
            "staged-locator":staged["staged-locator"],
            "staged-proof":staged["staged-proof"],
            "provider-payload":""
        })
    }

    fn publish_request(staged: &Value) -> Value {
        json!({
            "action":action(1,"publish","publish-a"),
            "staged-locator":staged["staged-locator"],
            "staged-proof":staged["staged-proof"],
            "write-completion-digest":"write-a",
            "create-policy":"fail-if-exists",
            "create-policy-digest":"policy-a",
            "provider-payload":publish_payload("/commit", vec![])
        })
    }

    async fn advance(
        client: &reqwest::Client,
        uri: &str,
        generation_value: u64,
    ) -> reqwest::Response {
        post_json(
            client,
            uri,
            "advance-fence",
            json!({"action":action(generation_value,"fence",&format!("lineage-{generation_value}"))}),
        )
        .await
    }

    async fn stage(client: &reqwest::Client, uri: &str, generation_value: u64) -> Value {
        let response = post_json(
            client,
            uri,
            "stage",
            json!({
                "action":action(generation_value,"stage","stage-digest"),
                "staged-identity":"staged-a",
                "initialization-digest":"init-a",
                "create-policy":"fail-if-exists",
                "create-policy-digest":"policy-a",
                "provider-payload":downstream_payload("/stage")
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        response.json().await.unwrap()
    }

    #[tokio::test]
    async fn next_action_fault_is_bounded_and_consumed_without_operation_id_guessing() {
        let temp = TempDir::new().unwrap();
        let downstream = downstream().await;
        let fixture = fixture(&downstream.uri, &temp.path().join("fixture.sqlite")).await;
        let client = reqwest::Client::new();

        let receipt: Value = client
            .post(format!("{}/_fixture/faults/next", fixture.uri))
            .json(&json!({"action":"advance-fence","fault":"before-accept"}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let arm_id = receipt["arm-id"].as_str().unwrap();
        assert_eq!(advance(&client, &fixture.uri, 1).await.status(), StatusCode::SERVICE_UNAVAILABLE);
        let cleared: Value = client
            .delete(format!("{}/_fixture/faults/next/{arm_id}", fixture.uri))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(cleared["entered"], true);

        let receipt: Value = client
            .post(format!("{}/_fixture/faults/next", fixture.uri))
            .json(&json!({"action":"stage","fault":"before-accept"}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let arm_id = receipt["arm-id"].as_str().unwrap();
        assert_eq!(advance(&client, &fixture.uri, 1).await.status(), StatusCode::OK);
        let cleared: Value = client
            .delete(format!("{}/_fixture/faults/next/{arm_id}", fixture.uri))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(cleared["entered"], false);
    }

    #[tokio::test]
    async fn config_and_standard_rest_are_transparently_proxied() {
        let temp = TempDir::new().unwrap();
        let downstream = downstream().await;
        let fixture = fixture(&downstream.uri, &temp.path().join("fixture.sqlite")).await;
        let client = reqwest::Client::new();

        let config: Value = client
            .get(format!("{}/v1/config", fixture.uri))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(config["defaults"][CAPABILITY], "1");
        assert_eq!(config["overrides"][CAPABILITY], "1");
        assert_eq!(config["defaults"]["warehouse"], "s3://warehouse");
        let namespaces: Value = client
            .get(format!("{}/v1/namespaces", fixture.uri))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(namespaces, json!({"namespaces":[["db"]]}));
    }

    #[tokio::test]
    async fn restart_preserves_stage_and_higher_generation_rejects_old_action() {
        let temp = TempDir::new().unwrap();
        let sqlite = temp.path().join("fixture.sqlite");
        let downstream = downstream().await;
        let client = reqwest::Client::new();
        let first = fixture(&downstream.uri, &sqlite).await;
        assert_eq!(
            advance(&client, &first.uri, 1).await.status(),
            StatusCode::OK
        );
        let staged = stage(&client, &first.uri, 1).await;
        assert_eq!(
            staged["staged-table"]["metadata-location"],
            "s3://warehouse/staged/metadata/00000.metadata.json"
        );
        drop(first);

        let restarted = fixture(&downstream.uri, &sqlite).await;
        let replayed = stage(&client, &restarted.uri, 1).await;
        assert_eq!(replayed, staged);
        assert_eq!(
            advance(&client, &restarted.uri, 2).await.status(),
            StatusCode::OK
        );
        let old_publish = post_json(
            &client,
            &restarted.uri,
            "publish",
            json!({
                "action":action(1,"publish","publish-a"),
                "staged-locator":staged["staged-locator"],
                "staged-proof":staged["staged-proof"],
                "write-completion-digest":"write-a",
                "create-policy":"fail-if-exists",
                "create-policy-digest":"policy-a",
                "provider-payload":publish_payload("/commit", vec![])
            }),
        )
        .await;
        assert_eq!(old_publish.status(), StatusCode::PRECONDITION_FAILED);
    }

    #[tokio::test]
    async fn restart_after_stage_uses_durable_cleanup_authority_without_abort_payload() {
        let temp = TempDir::new().unwrap();
        let sqlite = temp.path().join("fixture.sqlite");
        let storage = temp.path().join("cleanup-storage");
        std::fs::create_dir_all(&storage).unwrap();
        let cleanup_operator = Operator::new(
            opendal::services::Fs::default().root(storage.to_string_lossy().as_ref()),
        )
        .unwrap()
        .finish();
        cleanup_operator
            .write("staged/data/_staging/stage/part-a.parquet", b"a".to_vec())
            .await
            .unwrap();
        let downstream = downstream().await;
        let client = reqwest::Client::new();
        let first = fixture_with_cleanup_backend(
            &downstream.uri,
            &sqlite,
            CleanupBackend::Fixed(cleanup_operator.clone()),
        )
        .await;
        assert_eq!(
            advance(&client, &first.uri, 1).await.status(),
            StatusCode::OK
        );
        let staged = stage(&client, &first.uri, 1).await;
        drop(first);

        let restarted = fixture_with_cleanup_backend(
            &downstream.uri,
            &sqlite,
            CleanupBackend::Fixed(cleanup_operator.clone()),
        )
        .await;
        let abort = post_json(
            &client,
            &restarted.uri,
            "abort",
            json!({
                "action":action(1,"abort","abort-a"),
                "staged-locator":staged["staged-locator"],
                "staged-proof":staged["staged-proof"],
                "provider-payload":""
            }),
        )
        .await;
        assert_eq!(abort.status(), StatusCode::OK);
        assert_eq!(
            cleanup_operator
                .stat("staged/data/_staging/stage/part-a.parquet")
                .await
                .unwrap_err()
                .kind(),
            opendal::ErrorKind::NotFound
        );
        let inspection: Value = post_json(
            &client,
            &restarted.uri,
            "inspect",
            json!({
                "operation":operation(),
                "generation":generation(1),
                "input-digest":"lineage-1"
            }),
        )
        .await
        .json()
        .await
        .unwrap();
        assert_eq!(inspection["state"], "aborted");
    }

    #[tokio::test]
    async fn concurrent_publish_and_abort_have_one_durable_terminal() {
        let temp = TempDir::new().unwrap();
        let downstream = downstream().await;
        let sqlite = temp.path().join("fixture.sqlite");
        let storage = temp.path().join("cleanup-storage");
        std::fs::create_dir_all(&storage).unwrap();
        let cleanup_operator = Operator::new(
            opendal::services::Fs::default().root(storage.to_string_lossy().as_ref()),
        )
        .unwrap()
        .finish();
        let first = fixture_with_cleanup_backend(
            &downstream.uri,
            &sqlite,
            CleanupBackend::Fixed(cleanup_operator.clone()),
        )
        .await;
        let second = fixture_with_cleanup_backend(
            &downstream.uri,
            &sqlite,
            CleanupBackend::Fixed(cleanup_operator),
        )
        .await;
        let client = reqwest::Client::new();
        assert_eq!(
            advance(&client, &first.uri, 1).await.status(),
            StatusCode::OK
        );
        let staged = stage(&client, &first.uri, 1).await;
        let publish = post_json(
            &client,
            &first.uri,
            "publish",
            json!({
                "action":action(1,"publish","publish-a"),
                "staged-locator":staged["staged-locator"],
                "staged-proof":staged["staged-proof"],
                "write-completion-digest":"write-a",
                "create-policy":"fail-if-exists",
                "create-policy-digest":"policy-a",
                "provider-payload":publish_payload("/commit", vec![])
            }),
        );
        let abort = post_json(
            &client,
            &second.uri,
            "abort",
            json!({
                "action":action(1,"abort","abort-a"),
                "staged-locator":staged["staged-locator"],
                "staged-proof":staged["staged-proof"],
                "provider-payload":""
            }),
        );
        let (publish, abort) = tokio::join!(publish, abort);
        let statuses = [publish.status(), abort.status()];
        assert_eq!(
            statuses.iter().filter(|status| status.is_success()).count(),
            1
        );
        assert_eq!(
            statuses
                .iter()
                .filter(|status| **status == StatusCode::CONFLICT)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn response_loss_and_corrupt_or_missing_records_fail_closed() {
        let temp = TempDir::new().unwrap();
        let downstream = downstream().await;
        let proxy = fixture(&downstream.uri, &temp.path().join("fixture.sqlite")).await;
        let client = reqwest::Client::new();
        assert_eq!(
            advance(&client, &proxy.uri, 1).await.status(),
            StatusCode::OK
        );
        let staged = stage(&client, &proxy.uri, 1).await;
        client
            .post(format!("{}/_fixture/faults/operation-a", proxy.uri))
            .json(&json!({"fault":"after-downstream-before-response"}))
            .send()
            .await
            .unwrap();
        let request = json!({
            "action":action(1,"publish","publish-a"),
            "staged-locator":staged["staged-locator"],
            "staged-proof":staged["staged-proof"],
            "write-completion-digest":"write-a",
            "create-policy":"fail-if-exists",
            "create-policy-digest":"policy-a",
            "provider-payload":publish_payload("/commit", vec![])
        });
        assert_eq!(
            post_json(&client, &proxy.uri, "publish", request.clone())
                .await
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            post_json(&client, &proxy.uri, "publish", request)
                .await
                .status(),
            StatusCode::OK
        );

        client
            .post(format!("{}/_fixture/faults/operation-a", proxy.uri))
            .json(&json!({"fault":"record-corrupt"}))
            .send()
            .await
            .unwrap();
        let inspect: Value = post_json(
            &client,
            &proxy.uri,
            "inspect",
            json!({"operation":operation(),"generation":generation(1),"input-digest":"lineage-1"}),
        )
        .await
        .json()
        .await
        .unwrap();
        assert_eq!(inspect["state"], "ambiguous");

        let missing_fixture = fixture(&downstream.uri, &temp.path().join("missing.sqlite")).await;
        let inspect: Value = post_json(
            &client,
            &missing_fixture.uri,
            "inspect",
            json!({"operation":operation(),"generation":generation(1),"input-digest":"lineage-1"}),
        )
        .await
        .json()
        .await
        .unwrap();
        assert_eq!(inspect["state"], "ambiguous");
    }

    #[tokio::test]
    async fn accept_boundary_faults_are_typed_for_every_catalog_action() {
        let temp = TempDir::new().unwrap();
        let downstream = downstream().await;
        let client = reqwest::Client::new();

        for fault in ["before-accept", "after-accept"] {
            let proxy = fixture(
                &downstream.uri,
                &temp.path().join(format!("advance-{fault}.sqlite")),
            )
            .await;
            inject_fault(&client, &proxy.uri, fault).await;
            assert_eq!(
                advance(&client, &proxy.uri, 1).await.status(),
                StatusCode::SERVICE_UNAVAILABLE
            );
            let replay = advance(&client, &proxy.uri, 1).await;
            if fault == "before-accept" {
                assert_eq!(replay.status(), StatusCode::OK);
            } else {
                assert_eq!(replay.status(), StatusCode::OK);
                let body: Value = replay.json().await.unwrap();
                assert_eq!(body["generation"], generation(1));
            }
        }

        for fault in ["before-accept", "after-accept"] {
            let proxy = fixture(
                &downstream.uri,
                &temp.path().join(format!("stage-{fault}.sqlite")),
            )
            .await;
            assert_eq!(
                advance(&client, &proxy.uri, 1).await.status(),
                StatusCode::OK
            );
            inject_fault(&client, &proxy.uri, fault).await;
            assert_eq!(
                post_json(&client, &proxy.uri, "stage", stage_request())
                    .await
                    .status(),
                StatusCode::SERVICE_UNAVAILABLE
            );
            let replay = post_json(&client, &proxy.uri, "stage", stage_request()).await;
            if fault == "before-accept" {
                assert_eq!(replay.status(), StatusCode::OK);
            } else {
                assert_eq!(replay.status(), StatusCode::CONFLICT);
                let inspection: Value = post_json(
                    &client,
                    &proxy.uri,
                    "inspect",
                    json!({"operation":operation(),"generation":generation(1),"input-digest":"lineage-1"}),
                )
                .await
                .json()
                .await
                .unwrap();
                assert_eq!(inspection["state"], "ambiguous");
            }
        }

        for fault in ["before-accept", "after-accept"] {
            let proxy = fixture(
                &downstream.uri,
                &temp.path().join(format!("publish-{fault}.sqlite")),
            )
            .await;
            assert_eq!(
                advance(&client, &proxy.uri, 1).await.status(),
                StatusCode::OK
            );
            let staged = stage(&client, &proxy.uri, 1).await;
            inject_fault(&client, &proxy.uri, fault).await;
            assert_eq!(
                post_json(&client, &proxy.uri, "publish", publish_request(&staged))
                    .await
                    .status(),
                StatusCode::SERVICE_UNAVAILABLE
            );
            let replay = post_json(&client, &proxy.uri, "publish", publish_request(&staged)).await;
            if fault == "before-accept" {
                assert_eq!(replay.status(), StatusCode::OK);
            } else {
                assert_eq!(replay.status(), StatusCode::CONFLICT);
                let inspection: Value = post_json(
                    &client,
                    &proxy.uri,
                    "inspect",
                    json!({"operation":operation(),"generation":generation(1),"input-digest":"lineage-1"}),
                )
                .await
                .json()
                .await
                .unwrap();
                assert_eq!(inspection["state"], "ambiguous");
            }
        }

        for fault in ["before-accept", "after-accept"] {
            let storage = temp.path().join(format!("abort-{fault}-storage"));
            std::fs::create_dir_all(&storage).unwrap();
            let cleanup_operator = Operator::new(
                opendal::services::Fs::default().root(storage.to_string_lossy().as_ref()),
            )
            .unwrap()
            .finish();
            let proxy = fixture_with_cleanup_backend(
                &downstream.uri,
                &temp.path().join(format!("abort-{fault}.sqlite")),
                CleanupBackend::Fixed(cleanup_operator),
            )
            .await;
            assert_eq!(
                advance(&client, &proxy.uri, 1).await.status(),
                StatusCode::OK
            );
            let staged = stage(&client, &proxy.uri, 1).await;
            inject_fault(&client, &proxy.uri, fault).await;
            assert_eq!(
                post_json(&client, &proxy.uri, "abort", abort_request(&staged))
                    .await
                    .status(),
                StatusCode::SERVICE_UNAVAILABLE
            );
            let replay = post_json(&client, &proxy.uri, "abort", abort_request(&staged)).await;
            if fault == "before-accept" {
                assert_eq!(replay.status(), StatusCode::OK);
            } else {
                assert_eq!(replay.status(), StatusCode::CONFLICT);
                let inspection: Value = post_json(
                    &client,
                    &proxy.uri,
                    "inspect",
                    json!({"operation":operation(),"generation":generation(1),"input-digest":"lineage-1"}),
                )
                .await
                .json()
                .await
                .unwrap();
                assert_eq!(inspection["state"], "ambiguous");
            }
        }
    }

    #[tokio::test]
    async fn abort_downstream_faults_remain_durable_across_restart() {
        for fault in [
            "after-downstream-before-terminal",
            "after-downstream-before-response",
        ] {
            let temp = TempDir::new().unwrap();
            let downstream = downstream().await;
            let sqlite = temp.path().join("fixture.sqlite");
            let storage = temp.path().join("cleanup-storage");
            std::fs::create_dir_all(&storage).unwrap();
            let cleanup_operator = Operator::new(
                opendal::services::Fs::default().root(storage.to_string_lossy().as_ref()),
            )
            .unwrap()
            .finish();
            let client = reqwest::Client::new();
            let first = fixture_with_cleanup_backend(
                &downstream.uri,
                &sqlite,
                CleanupBackend::Fixed(cleanup_operator.clone()),
            )
            .await;
            assert_eq!(
                advance(&client, &first.uri, 1).await.status(),
                StatusCode::OK
            );
            let staged = stage(&client, &first.uri, 1).await;
            inject_fault(&client, &first.uri, fault).await;
            assert_eq!(
                post_json(&client, &first.uri, "abort", abort_request(&staged))
                    .await
                    .status(),
                StatusCode::SERVICE_UNAVAILABLE
            );
            drop(first);

            let restarted = fixture_with_cleanup_backend(
                &downstream.uri,
                &sqlite,
                CleanupBackend::Fixed(cleanup_operator),
            )
            .await;
            let inspection: Value = post_json(
                &client,
                &restarted.uri,
                "inspect",
                json!({"operation":operation(),"generation":generation(1),"input-digest":"lineage-1"}),
            )
            .await
            .json()
            .await
            .unwrap();
            if fault == "after-downstream-before-terminal" {
                assert_eq!(inspection["state"], "ambiguous");
                assert_eq!(
                    post_json(&client, &restarted.uri, "abort", abort_request(&staged))
                        .await
                        .status(),
                    StatusCode::CONFLICT
                );
            } else {
                assert_eq!(inspection["state"], "aborted");
                assert_eq!(
                    post_json(&client, &restarted.uri, "abort", abort_request(&staged))
                        .await
                        .status(),
                    StatusCode::OK
                );
            }
        }
    }

    #[tokio::test]
    async fn delayed_old_request_cannot_cross_a_higher_fence() {
        let temp = TempDir::new().unwrap();
        let downstream = downstream().await;
        let sqlite = temp.path().join("fixture.sqlite");
        let first = fixture(&downstream.uri, &sqlite).await;
        let second = fixture(&downstream.uri, &sqlite).await;
        let client = reqwest::Client::new();
        assert_eq!(
            advance(&client, &first.uri, 1).await.status(),
            StatusCode::OK
        );
        let staged = stage(&client, &first.uri, 1).await;
        client
            .post(format!("{}/_fixture/faults/operation-a", second.uri))
            .json(&json!({"fault":"delayed-old-request"}))
            .send()
            .await
            .unwrap();
        let delay_entered = second.delay_entered.as_ref().unwrap().notified();
        let old_client = client.clone();
        let old_uri = second.uri.clone();
        let old = tokio::spawn(async move {
            post_json(
                &old_client,
                &old_uri,
                "publish",
                json!({
                "action":action(1,"publish","publish-a"),
                "staged-locator":staged["staged-locator"],
                "staged-proof":staged["staged-proof"],
                "write-completion-digest":"write-a",
                "create-policy":"fail-if-exists",
                "create-policy-digest":"policy-a",
                "provider-payload":publish_payload("/commit", vec![])
                }),
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(5), delay_entered)
            .await
            .expect("old publish entered delayed fault");
        let higher = advance(&client, &first.uri, 2).await;
        let old = old.await.expect("old publish task");
        assert_eq!(higher.status(), StatusCode::OK);
        assert_eq!(old.status(), StatusCode::PRECONDITION_FAILED);
    }

    #[tokio::test]
    async fn delayed_old_stage_and_abort_cannot_cross_a_higher_fence() {
        let temp = TempDir::new().unwrap();
        let downstream = downstream().await;
        let client = reqwest::Client::new();

        let stage_sqlite = temp.path().join("stage.sqlite");
        let stage_first = fixture(&downstream.uri, &stage_sqlite).await;
        let stage_second = fixture(&downstream.uri, &stage_sqlite).await;
        assert_eq!(
            advance(&client, &stage_first.uri, 1).await.status(),
            StatusCode::OK
        );
        inject_fault(&client, &stage_second.uri, "delayed-old-request").await;
        let delay_entered = stage_second.delay_entered.as_ref().unwrap().notified();
        let old_client = client.clone();
        let old_uri = stage_second.uri.clone();
        let old_stage = tokio::spawn(async move {
            post_json(&old_client, &old_uri, "stage", stage_request()).await
        });
        tokio::time::timeout(Duration::from_secs(5), delay_entered)
            .await
            .expect("old stage entered delayed fault");
        let higher = advance(&client, &stage_first.uri, 2).await;
        let old_stage = old_stage.await.expect("old stage task");
        assert_eq!(higher.status(), StatusCode::OK);
        assert_eq!(old_stage.status(), StatusCode::PRECONDITION_FAILED);

        let abort_sqlite = temp.path().join("abort.sqlite");
        let storage = temp.path().join("abort-storage");
        std::fs::create_dir_all(&storage).unwrap();
        let cleanup_operator = Operator::new(
            opendal::services::Fs::default().root(storage.to_string_lossy().as_ref()),
        )
        .unwrap()
        .finish();
        let abort_first = fixture_with_cleanup_backend(
            &downstream.uri,
            &abort_sqlite,
            CleanupBackend::Fixed(cleanup_operator.clone()),
        )
        .await;
        let abort_second = fixture_with_cleanup_backend(
            &downstream.uri,
            &abort_sqlite,
            CleanupBackend::Fixed(cleanup_operator),
        )
        .await;
        assert_eq!(
            advance(&client, &abort_first.uri, 1).await.status(),
            StatusCode::OK
        );
        let staged = stage(&client, &abort_first.uri, 1).await;
        inject_fault(&client, &abort_second.uri, "delayed-old-request").await;
        let delay_entered = abort_second.delay_entered.as_ref().unwrap().notified();
        let old_client = client.clone();
        let old_uri = abort_second.uri.clone();
        let old_abort = tokio::spawn(async move {
            post_json(&old_client, &old_uri, "abort", abort_request(&staged)).await
        });
        tokio::time::timeout(Duration::from_secs(5), delay_entered)
            .await
            .expect("old abort entered delayed fault");
        let higher = advance(&client, &abort_first.uri, 2).await;
        let old_abort = old_abort.await.expect("old abort task");
        assert_eq!(higher.status(), StatusCode::OK);
        assert_eq!(old_abort.status(), StatusCode::PRECONDITION_FAILED);
    }

    #[tokio::test]
    async fn downstream_commit_before_sqlite_terminal_remains_ambiguous_after_restart() {
        let temp = TempDir::new().unwrap();
        let sqlite = temp.path().join("fixture.sqlite");
        let downstream = downstream().await;
        let client = reqwest::Client::new();
        let first = fixture(&downstream.uri, &sqlite).await;
        assert_eq!(
            advance(&client, &first.uri, 1).await.status(),
            StatusCode::OK
        );
        let staged = stage(&client, &first.uri, 1).await;
        client
            .post(format!("{}/_fixture/faults/operation-a", first.uri))
            .json(&json!({"fault":"after-downstream-before-terminal"}))
            .send()
            .await
            .unwrap();
        let request = json!({
            "action":action(1,"publish","publish-a"),
            "staged-locator":staged["staged-locator"],
            "staged-proof":staged["staged-proof"],
            "write-completion-digest":"write-a",
            "create-policy":"fail-if-exists",
            "create-policy-digest":"policy-a",
            "provider-payload":publish_payload("/commit", vec![])
        });
        assert_eq!(
            post_json(&client, &first.uri, "publish", request.clone())
                .await
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        let inspection: Value = post_json(
            &client,
            &first.uri,
            "inspect",
            json!({"operation":operation(),"generation":generation(1),"input-digest":"lineage-1"}),
        )
        .await
        .json()
        .await
        .unwrap();
        assert_eq!(inspection["state"], "ambiguous");
        assert_eq!(
            inspection["message"],
            "catalog action remains durably in flight"
        );
        assert!(inspection["proof"].as_str().is_some());
        assert_eq!(
            advance(&client, &first.uri, 2).await.status(),
            StatusCode::CONFLICT
        );
        drop(first);

        let restarted = fixture(&downstream.uri, &sqlite).await;
        let inspection: Value = post_json(
            &client,
            &restarted.uri,
            "inspect",
            json!({"operation":operation(),"generation":generation(1),"input-digest":"lineage-1"}),
        )
        .await
        .json()
        .await
        .unwrap();
        assert_eq!(inspection["state"], "ambiguous");
        assert_eq!(
            post_json(&client, &restarted.uri, "publish", request)
                .await
                .status(),
            StatusCode::CONFLICT
        );
    }

    #[tokio::test]
    async fn stage_downstream_commit_before_durable_result_is_ambiguous_after_restart() {
        let temp = TempDir::new().unwrap();
        let sqlite = temp.path().join("fixture.sqlite");
        let downstream = downstream().await;
        let client = reqwest::Client::new();
        let first = fixture(&downstream.uri, &sqlite).await;
        assert_eq!(
            advance(&client, &first.uri, 1).await.status(),
            StatusCode::OK
        );
        client
            .post(format!("{}/_fixture/faults/operation-a", first.uri))
            .json(&json!({"fault":"after-downstream-before-terminal"}))
            .send()
            .await
            .unwrap();
        let stage_request = json!({
            "action":action(1,"stage","stage-digest"),
            "staged-identity":"staged-a",
            "initialization-digest":"init-a",
            "create-policy":"fail-if-exists",
            "create-policy-digest":"policy-a",
            "provider-payload":downstream_payload("/stage")
        });
        assert_eq!(
            post_json(&client, &first.uri, "stage", stage_request.clone())
                .await
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        drop(first);
        let restarted = fixture(&downstream.uri, &sqlite).await;
        let inspection: Value = post_json(
            &client,
            &restarted.uri,
            "inspect",
            json!({"operation":operation(),"generation":generation(1),"input-digest":"lineage-1"}),
        )
        .await
        .json()
        .await
        .unwrap();
        assert_eq!(inspection["state"], "ambiguous");
        assert_eq!(
            post_json(&client, &restarted.uri, "stage", stage_request)
                .await
                .status(),
            StatusCode::CONFLICT
        );
        assert_eq!(
            advance(&client, &restarted.uri, 2).await.status(),
            StatusCode::CONFLICT
        );
    }

    #[tokio::test]
    async fn explicit_no_op_policy_is_durable_and_historical() {
        let temp = TempDir::new().unwrap();
        let downstream = downstream().await;
        let sqlite = temp.path().join("fixture.sqlite");
        let storage = temp.path().join("cleanup-storage");
        std::fs::create_dir_all(&storage).unwrap();
        let cleanup_operator = Operator::new(
            opendal::services::Fs::default().root(storage.to_string_lossy().as_ref()),
        )
        .unwrap()
        .finish();
        cleanup_operator
            .write("staged/data/_staging/stage/part-a.parquet", b"a".to_vec())
            .await
            .unwrap();
        cleanup_operator
            .write(
                "staged/data/_staging/stage/nested/part-b.parquet",
                b"b".to_vec(),
            )
            .await
            .unwrap();
        cleanup_operator
            .write("staged/metadata/orphan.avro", b"c".to_vec())
            .await
            .unwrap();
        cleanup_operator
            .write("keep/retained", b"keep".to_vec())
            .await
            .unwrap();
        let first = fixture_with_cleanup_backend(
            &downstream.uri,
            &sqlite,
            CleanupBackend::Fixed(cleanup_operator.clone()),
        )
        .await;
        let client = reqwest::Client::new();
        assert_eq!(
            advance(&client, &first.uri, 1).await.status(),
            StatusCode::OK
        );
        let stage_response = post_json(
            &client,
            &first.uri,
            "stage",
            json!({
                "action":action(1,"stage","stage-digest"),
                "staged-identity":"staged-a",
                "initialization-digest":"init-a",
                "create-policy":"no-op-if-exists",
                "create-policy-digest":"policy-a",
                "provider-payload":downstream_payload("/stage")
            }),
        )
        .await;
        assert_eq!(stage_response.status(), StatusCode::OK);
        let staged: Value = stage_response.json().await.unwrap();
        let published: Value = post_json(
            &client,
            &first.uri,
            "publish",
            json!({
                "action":action(1,"publish","publish-a"),
                "staged-locator":staged["staged-locator"],
                "staged-proof":staged["staged-proof"],
                "write-completion-digest":"write-a",
                "create-policy":"no-op-if-exists",
                "create-policy-digest":"policy-a",
                "provider-payload":publish_payload(
                    "/already-exists",
                    vec!["s3://warehouse/staged/metadata/orphan.avro"]
                )
            }),
        )
        .await
        .json()
        .await
        .unwrap();
        assert_eq!(published["disposition"], "no-op");
        let inspection: Value = post_json(
            &client,
            &first.uri,
            "inspect",
            json!({"operation":operation(),"generation":generation(1),"input-digest":"lineage-1"}),
        )
        .await
        .json()
        .await
        .unwrap();
        assert_eq!(inspection["state"], "no-op");
        assert_eq!(inspection["staged-locator"], staged["staged-locator"]);
        assert_eq!(inspection["staged-proof"], staged["staged-proof"]);

        let cleanup: Value = post_json(
            &client,
            &first.uri,
            "abort",
            json!({
                "action":action(1,"cleanup","cleanup-a"),
                "staged-locator":staged["staged-locator"],
                "staged-proof":staged["staged-proof"],
                "provider-payload":""
            }),
        )
        .await
        .json()
        .await
        .unwrap();
        assert!(cleanup["proof"].as_str().is_some());
        assert_eq!(
            cleanup_operator
                .stat("staged/data/_staging/stage/part-a.parquet")
                .await
                .unwrap_err()
                .kind(),
            opendal::ErrorKind::NotFound
        );
        assert_eq!(
            cleanup_operator
                .stat("staged/data/_staging/stage/nested/part-b.parquet")
                .await
                .unwrap_err()
                .kind(),
            opendal::ErrorKind::NotFound
        );
        assert_eq!(
            cleanup_operator
                .stat("staged/metadata/orphan.avro")
                .await
                .unwrap_err()
                .kind(),
            opendal::ErrorKind::NotFound
        );
        assert!(cleanup_operator.stat("keep/retained").await.is_ok());
        let inspection: Value = post_json(
            &client,
            &first.uri,
            "inspect",
            json!({"operation":operation(),"generation":generation(1),"input-digest":"lineage-1"}),
        )
        .await
        .json()
        .await
        .unwrap();
        assert_eq!(inspection["state"], "no-op");
        assert!(inspection.get("staged-locator").is_none());
        let cleanup_replay = post_json(
            &client,
            &first.uri,
            "abort",
            json!({
                "action":action(1,"cleanup","cleanup-a"),
                "staged-locator":staged["staged-locator"],
                "staged-proof":staged["staged-proof"],
                "provider-payload":""
            }),
        )
        .await;
        assert_eq!(cleanup_replay.status(), StatusCode::OK);
        drop(first);
        let restarted = fixture(&downstream.uri, &sqlite).await;
        let cleanup_replay = post_json(
            &client,
            &restarted.uri,
            "abort",
            json!({
                "action":action(1,"cleanup","cleanup-a"),
                "staged-locator":staged["staged-locator"],
                "staged-proof":staged["staged-proof"],
                "provider-payload":""
            }),
        )
        .await;
        assert_eq!(cleanup_replay.status(), StatusCode::OK);
        let drifted_payload = post_json(
            &client,
            &restarted.uri,
            "abort",
            json!({
                "action":action(1,"cleanup","cleanup-a"),
                "staged-locator":staged["staged-locator"],
                "staged-proof":staged["staged-proof"],
                "provider-payload":json!({
                    "kind":"iceberg-cleanup-v1",
                    "data-prefixes":["s3://warehouse/staged/data/_staging/stage/"],
                    "objects":["s3://warehouse/staged/metadata/foreign.avro"]
                }).to_string()
            }),
        )
        .await;
        assert_eq!(drifted_payload.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn cleanup_failure_is_known_rejection_and_does_not_finalize_abort() {
        let temp = TempDir::new().unwrap();
        let downstream = downstream().await;
        let sqlite = temp.path().join("fixture.sqlite");
        let fixture = fixture_with_cleanup_backend(
            &downstream.uri,
            &sqlite,
            CleanupBackend::Failing("injected cleanup failure".to_string()),
        )
        .await;
        let client = reqwest::Client::new();
        assert_eq!(
            advance(&client, &fixture.uri, 1).await.status(),
            StatusCode::OK
        );
        let stage_response = post_json(
            &client,
            &fixture.uri,
            "stage",
            json!({
                "action":action(1,"stage","stage-digest"),
                "staged-identity":"staged-a",
                "initialization-digest":"init-a",
                "create-policy":"no-op-if-exists",
                "create-policy-digest":"policy-a",
                "provider-payload":downstream_payload("/stage")
            }),
        )
        .await;
        assert_eq!(stage_response.status(), StatusCode::OK);
        let staged: Value = stage_response.json().await.unwrap();
        let publish_response = post_json(
            &client,
            &fixture.uri,
            "publish",
            json!({
                "action":action(1,"publish","publish-a"),
                "staged-locator":staged["staged-locator"],
                "staged-proof":staged["staged-proof"],
                "write-completion-digest":"write-a",
                "create-policy":"no-op-if-exists",
                "create-policy-digest":"policy-a",
                "provider-payload":publish_payload("/already-exists", vec![])
            }),
        )
        .await;
        assert_eq!(publish_response.status(), StatusCode::OK);

        let cleanup_response = post_json(
            &client,
            &fixture.uri,
            "abort",
            json!({
                "action":action(1,"cleanup","cleanup-a"),
                "staged-locator":staged["staged-locator"],
                "staged-proof":staged["staged-proof"],
                "provider-payload":""
            }),
        )
        .await;
        assert_eq!(cleanup_response.status(), StatusCode::CONFLICT);
        let inspection: Value = post_json(
            &client,
            &fixture.uri,
            "inspect",
            json!({
                "operation":operation(),
                "generation":generation(1),
                "input-digest":"lineage-1"
            }),
        )
        .await
        .json()
        .await
        .unwrap();
        assert_eq!(inspection["state"], "no-op");
        assert_eq!(inspection["staged-locator"], staged["staged-locator"]);
    }

    #[test]
    fn cleanup_payload_rejects_credentials_traversal_and_excess() {
        assert!(validate_cleanup_payload(&["data/".to_string()], &[]).is_ok());
        assert!(
            validate_cleanup_payload(&["s3://user:secret@bucket/data".to_string()], &[]).is_err()
        );
        assert!(
            validate_cleanup_payload(&["s3://bucket/data?secret=value".to_string()], &[]).is_err()
        );
        assert!(validate_cleanup_payload(&["../data".to_string()], &[]).is_err());
        assert!(validate_cleanup_payload(&[".".to_string()], &[]).is_err());
        assert!(
            validate_cleanup_payload(&vec!["data".to_string(); MAX_CLEANUP_ITEMS + 1], &[])
                .is_err()
        );
        assert!(validate_cleanup_payload(&["x".repeat(MAX_CLEANUP_PATH_BYTES + 1)], &[]).is_err());
        assert!(
            serde_json::from_value::<DownstreamAction>(json!({
                "kind":"iceberg-cleanup-v1",
                "data-prefixes":["data/"],
                "objects":[],
                "access-key":"must-not-be-accepted"
            }))
            .is_err()
        );
        let durable = derive_stage_cleanup_authority(
            &json!({
                "metadata-location":null,
                "metadata":{
                    "location":"s3://warehouse/db/table",
                    "table-uuid":"00000000-0000-4000-8000-000000000002"
                }
            }),
            "01234567-89ab-cdef-0123-456789abcdef",
        )
        .unwrap();
        assert_eq!(
            durable.data_prefixes,
            vec![
                "s3://warehouse/db/table/data/_staging/01234567-89ab-cdef-0123-456789abcdef/"
                    .to_string()
            ]
        );
        let drift = json!({
            "kind":"iceberg-publish-v1",
            "action":{"method":"POST","path":"/commit","body":{}},
            "data-prefixes":["s3://warehouse/foreign/data/_staging/action/"],
            "objects":[]
        })
        .to_string();
        match parse_publish_action(&drift, &durable) {
            Err(response) => assert_eq!(response.status(), StatusCode::CONFLICT),
            Ok(_) => panic!("publish cleanup authority drift must fail closed"),
        }
    }

    #[tokio::test]
    async fn published_target_is_never_cleanup_authority() {
        let temp = TempDir::new().unwrap();
        let downstream = downstream().await;
        let fixture = fixture(&downstream.uri, &temp.path().join("fixture.sqlite")).await;
        let client = reqwest::Client::new();
        assert_eq!(
            advance(&client, &fixture.uri, 1).await.status(),
            StatusCode::OK
        );
        let staged = stage(&client, &fixture.uri, 1).await;
        assert_eq!(
            post_json(
                &client,
                &fixture.uri,
                "publish",
                json!({
                    "action":action(1,"publish","publish-a"),
                    "staged-locator":staged["staged-locator"],
                    "staged-proof":staged["staged-proof"],
                    "write-completion-digest":"write-a",
                    "create-policy":"fail-if-exists",
                    "create-policy-digest":"policy-a",
                    "provider-payload":publish_payload("/commit", vec![])
                }),
            )
            .await
            .status(),
            StatusCode::OK
        );
        assert_eq!(
            post_json(
                &client,
                &fixture.uri,
                "abort",
                json!({
                    "action":action(1,"cleanup","cleanup-a"),
                    "staged-locator":staged["staged-locator"],
                    "staged-proof":staged["staged-proof"],
                    "provider-payload":""
                }),
            )
            .await
            .status(),
            StatusCode::CONFLICT
        );
    }

    #[tokio::test]
    async fn stage_response_loss_preserves_a_durable_staged_observation() {
        let temp = TempDir::new().unwrap();
        let downstream = downstream().await;
        let fixture = fixture(&downstream.uri, &temp.path().join("fixture.sqlite")).await;
        let client = reqwest::Client::new();
        assert_eq!(
            advance(&client, &fixture.uri, 1).await.status(),
            StatusCode::OK
        );
        client
            .post(format!("{}/_fixture/faults/operation-a", fixture.uri))
            .json(&json!({"fault":"after-downstream-before-response"}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            post_json(
                &client,
                &fixture.uri,
                "stage",
                json!({
                    "action":action(1,"stage","stage-digest"),
                    "staged-identity":"staged-a",
                    "initialization-digest":"init-a",
                    "create-policy":"fail-if-exists",
                    "create-policy-digest":"policy-a",
                    "provider-payload":downstream_payload("/stage")
                }),
            )
            .await
            .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        let inspection: Value = post_json(
            &client,
            &fixture.uri,
            "inspect",
            json!({"operation":operation(),"generation":generation(1),"input-digest":"lineage-1"}),
        )
        .await
        .json()
        .await
        .unwrap();
        assert_eq!(inspection["state"], "staged");
    }

    #[tokio::test]
    async fn unknown_durable_protocol_is_unsupported_and_never_mutated() {
        let temp = TempDir::new().unwrap();
        let downstream = downstream().await;
        let sqlite = temp.path().join("fixture.sqlite");
        let fixture = fixture(&downstream.uri, &sqlite).await;
        let client = reqwest::Client::new();
        assert_eq!(
            advance(&client, &fixture.uri, 1).await.status(),
            StatusCode::OK
        );
        let connection = Connection::open(&sqlite).unwrap();
        connection
            .execute(
                "UPDATE ctas_operations SET record_json=replace(record_json, '\"protocol_version\":1', '\"protocol_version\":2')",
                [],
            )
            .unwrap();
        let inspection: Value = post_json(
            &client,
            &fixture.uri,
            "inspect",
            json!({"operation":operation(),"generation":generation(1),"input-digest":"lineage-1"}),
        )
        .await
        .json()
        .await
        .unwrap();
        assert_eq!(inspection["state"], "unsupported");
        assert_eq!(
            advance(&client, &fixture.uri, 2).await.status(),
            StatusCode::NOT_IMPLEMENTED
        );
        let raw: String = connection
            .query_row("SELECT record_json FROM ctas_operations", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert!(raw.contains("\"protocol_version\":2"));
        let raw: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(raw["generation"]["fence-generation"], 1);
    }

    #[tokio::test]
    async fn known_downstream_rejection_clears_durable_in_flight_state() {
        let temp = TempDir::new().unwrap();
        let downstream = downstream().await;
        let fixture = fixture(&downstream.uri, &temp.path().join("fixture.sqlite")).await;
        let client = reqwest::Client::new();
        assert_eq!(
            advance(&client, &fixture.uri, 1).await.status(),
            StatusCode::OK
        );
        let response = post_json(
            &client,
            &fixture.uri,
            "stage",
            json!({
                "action":action(1,"stage","stage-digest"),
                "staged-identity":"staged-a",
                "initialization-digest":"init-a",
                "create-policy":"fail-if-exists",
                "create-policy-digest":"policy-a",
                "provider-payload":publish_payload("/conflict", vec![])
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let inspection: Value = post_json(
            &client,
            &fixture.uri,
            "inspect",
            json!({"operation":operation(),"generation":generation(1),"input-digest":"lineage-1"}),
        )
        .await
        .json()
        .await
        .unwrap();
        assert_eq!(inspection["state"], "not-created");
        assert_eq!(
            advance(&client, &fixture.uri, 2).await.status(),
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn if_not_exists_does_not_turn_a_generic_conflict_into_no_op() {
        let temp = TempDir::new().unwrap();
        let downstream = downstream().await;
        let fixture = fixture(&downstream.uri, &temp.path().join("fixture.sqlite")).await;
        let client = reqwest::Client::new();
        assert_eq!(
            advance(&client, &fixture.uri, 1).await.status(),
            StatusCode::OK
        );
        let staged: Value = post_json(
            &client,
            &fixture.uri,
            "stage",
            json!({
                "action":action(1,"stage","stage-digest"),
                "staged-identity":"staged-a",
                "initialization-digest":"init-a",
                "create-policy":"no-op-if-exists",
                "create-policy-digest":"policy-a",
                "provider-payload":downstream_payload("/stage")
            }),
        )
        .await
        .json()
        .await
        .unwrap();
        let response = post_json(
            &client,
            &fixture.uri,
            "publish",
            json!({
                "action":action(1,"publish","publish-a"),
                "staged-locator":staged["staged-locator"],
                "staged-proof":staged["staged-proof"],
                "write-completion-digest":"write-a",
                "create-policy":"no-op-if-exists",
                "create-policy-digest":"policy-a",
                "provider-payload":publish_payload("/conflict", vec![])
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let inspection: Value = post_json(
            &client,
            &fixture.uri,
            "inspect",
            json!({"operation":operation(),"generation":generation(1),"input-digest":"lineage-1"}),
        )
        .await
        .json()
        .await
        .unwrap();
        assert_eq!(inspection["state"], "staged");
    }

    #[tokio::test]
    async fn if_not_exists_ignores_already_exists_message_without_standard_type() {
        let temp = TempDir::new().unwrap();
        let downstream = downstream().await;
        let fixture = fixture(&downstream.uri, &temp.path().join("fixture.sqlite")).await;
        let client = reqwest::Client::new();
        assert_eq!(
            advance(&client, &fixture.uri, 1).await.status(),
            StatusCode::OK
        );
        let staged: Value = post_json(
            &client,
            &fixture.uri,
            "stage",
            json!({
                "action":action(1,"stage","stage-digest"),
                "staged-identity":"staged-a",
                "initialization-digest":"init-a",
                "create-policy":"no-op-if-exists",
                "create-policy-digest":"policy-a",
                "provider-payload":downstream_payload("/stage")
            }),
        )
        .await
        .json()
        .await
        .unwrap();
        let response = post_json(
            &client,
            &fixture.uri,
            "publish",
            json!({
                "action":action(1,"publish","publish-a"),
                "staged-locator":staged["staged-locator"],
                "staged-proof":staged["staged-proof"],
                "write-completion-digest":"write-a",
                "create-policy":"no-op-if-exists",
                "create-policy-digest":"policy-a",
                "provider-payload":publish_payload("/misleading-conflict", vec![])
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let inspection: Value = post_json(
            &client,
            &fixture.uri,
            "inspect",
            json!({"operation":operation(),"generation":generation(1),"input-digest":"lineage-1"}),
        )
        .await
        .json()
        .await
        .unwrap();
        assert_eq!(inspection["state"], "staged");
    }

    #[tokio::test]
    async fn drop_recreate_replaces_durable_target_identity_and_forbids_cleanup_after_restart() {
        let temp = TempDir::new().unwrap();
        let sqlite = temp.path().join("fixture.sqlite");
        let storage = temp.path().join("storage");
        let operator = Operator::new(
            opendal::services::Fs::default().root(storage.to_string_lossy().as_ref()),
        )
        .unwrap()
        .finish();
        let downstream = downstream().await;
        let client = reqwest::Client::new();
        let first = fixture_with_cleanup_backend(
            &downstream.uri,
            &sqlite,
            CleanupBackend::Fixed(operator.clone()),
        )
        .await;
        assert_eq!(
            advance(&client, &first.uri, 1).await.status(),
            StatusCode::OK
        );
        let staged = stage(&client, &first.uri, 1).await;
        operator
            .write("staged/data/_staging/stage/part-a.parquet", b"a".to_vec())
            .await
            .unwrap();
        assert_eq!(
            client
                .post(format!("{}/_fixture/drop-recreate-target", first.uri))
                .json(&json!({
                    "operation":operation(),
                    "replacement-identity":"00000000-0000-4000-8000-000000000099"
                }))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NO_CONTENT
        );
        drop(first);

        let restarted = fixture_with_cleanup_backend(
            &downstream.uri,
            &sqlite,
            CleanupBackend::Fixed(operator.clone()),
        )
        .await;
        let inspect_request = json!({
            "operation":operation(),
            "generation":generation(1),
            "input-digest":"lineage-1"
        });
        let first_inspection: Value =
            post_json(&client, &restarted.uri, "inspect", inspect_request.clone())
                .await
                .json()
                .await
                .unwrap();
        let replayed_inspection: Value =
            post_json(&client, &restarted.uri, "inspect", inspect_request)
                .await
                .json()
                .await
                .unwrap();
        assert_eq!(first_inspection["state"], "conflict");
        assert_eq!(first_inspection["kind"], "identity-conflict");
        assert_eq!(first_inspection["proof"], replayed_inspection["proof"]);
        assert_eq!(
            post_json(&client, &restarted.uri, "abort", abort_request(&staged))
                .await
                .status(),
            StatusCode::CONFLICT
        );
        assert!(
            operator
                .stat("staged/data/_staging/stage/part-a.parquet")
                .await
                .is_ok(),
            "identity replacement must prevent external cleanup"
        );
    }

    #[tokio::test]
    async fn inspect_conflicts_are_conclusive_and_carry_deterministic_proofs() {
        let temp = TempDir::new().unwrap();
        let downstream = downstream().await;
        let fixture = fixture(&downstream.uri, &temp.path().join("fixture.sqlite")).await;
        let client = reqwest::Client::new();
        assert_eq!(
            advance(&client, &fixture.uri, 2).await.status(),
            StatusCode::OK
        );

        let stale_request = json!({
            "operation":operation(),
            "generation":generation(1),
            "input-digest":"lineage-2"
        });
        let stale_first: Value = post_json(&client, &fixture.uri, "inspect", stale_request.clone())
            .await
            .json()
            .await
            .unwrap();
        let stale_replay: Value = post_json(&client, &fixture.uri, "inspect", stale_request)
            .await
            .json()
            .await
            .unwrap();
        assert_eq!(stale_first["state"], "conflict");
        assert_eq!(stale_first["kind"], "stale-fence");
        assert_eq!(stale_first["proof"], stale_replay["proof"]);
        assert!(stale_first["proof"].as_str().unwrap().len() < 128);

        let digest_request = json!({
            "operation":operation(),
            "generation":generation(2),
            "input-digest":"foreign-lineage"
        });
        let digest_conflict: Value =
            post_json(&client, &fixture.uri, "inspect", digest_request.clone())
                .await
                .json()
                .await
                .unwrap();
        let digest_replay: Value = post_json(&client, &fixture.uri, "inspect", digest_request)
            .await
            .json()
            .await
            .unwrap();
        assert_eq!(digest_conflict["kind"], "digest-conflict");
        assert_eq!(digest_conflict["proof"], digest_replay["proof"]);
        assert!(digest_conflict["proof"].as_str().unwrap().len() < 128);

        let mut foreign_operation = operation();
        foreign_operation["target"]["name"] = Value::String("foreign-target".to_string());
        let identity_request = json!({
            "operation":foreign_operation,
            "generation":generation(2),
            "input-digest":"lineage-2"
        });
        let identity_conflict: Value =
            post_json(&client, &fixture.uri, "inspect", identity_request.clone())
                .await
                .json()
                .await
                .unwrap();
        let identity_replay: Value = post_json(&client, &fixture.uri, "inspect", identity_request)
            .await
            .json()
            .await
            .unwrap();
        assert_eq!(identity_conflict["kind"], "identity-conflict");
        assert_eq!(identity_conflict["proof"], identity_replay["proof"]);
        assert!(identity_conflict["proof"].as_str().unwrap().len() < 128);
    }
}
