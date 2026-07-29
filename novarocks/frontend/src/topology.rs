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

//! Frontend-owned durable backend membership and runtime topology.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::net::SocketAddr;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use novarocks::common::app_config::ClusterRole;
use novarocks::query_execution::backend::{
    BackendQueryEvent, BackendQueryEventSink, BackendTopologyError, BackendTopologyMetricsSnapshot,
    BackendTopologyPort, BackendTopologySnapshot, BackendTopologyValidationError, HeartbeatOutcome,
    LiveBackendTarget, publish_backend_topology_metrics,
};
use novarocks::runtime::query_result::{QueryResult, QueryResultColumn, record_batch_to_chunk};
use novarocks_spi::state_store::{
    CommitResolution, Key, Precondition, StateRecord, StateStore, Value,
};
use novarocks_state_store::{OperationId, RunFailure, run_side_effect_free};
use serde::{Deserialize, Serialize};
use tokio::runtime::{Handle, RuntimeFlavor};
use uuid::Uuid;

const CLUSTER_BACKENDS_KEY: &[u8] = b"novarocks/frontend/cluster-backends/v1/state";
const CLUSTER_BACKENDS_SCHEMA_VERSION: u8 = 1;

type HeartbeatProbe = dyn Fn(u32, SocketAddr) -> HeartbeatOutcome + Send + Sync + 'static;

/// Immutable configuration used to open the frontend membership owner.
///
/// The fields stay private so composition must validate endpoint identity once,
/// before the service can begin restoring or publishing topology.
#[derive(Clone, Debug)]
pub struct ClusterBackendOpenConfig {
    role: ClusterRole,
    seed_endpoints: Vec<SocketAddr>,
    heartbeat_interval: Duration,
    heartbeat_timeout_retries: u32,
    decommission_timeout: Duration,
}

impl ClusterBackendOpenConfig {
    pub fn new(
        role: ClusterRole,
        seed_endpoints: Vec<SocketAddr>,
        heartbeat_interval: Duration,
        heartbeat_timeout_retries: u32,
        decommission_timeout: Duration,
    ) -> Result<Self, String> {
        if heartbeat_interval.is_zero() {
            return Err("cluster backend heartbeat interval must be non-zero".to_string());
        }
        if heartbeat_timeout_retries == 0 {
            return Err("cluster backend heartbeat timeout retries must be non-zero".to_string());
        }
        if decommission_timeout.is_zero() {
            return Err("cluster backend decommission timeout must be non-zero".to_string());
        }
        let mut seen = BTreeSet::new();
        for endpoint in &seed_endpoints {
            if endpoint.to_string().parse::<SocketAddr>().ok() != Some(*endpoint) {
                return Err(format!(
                    "cluster backend endpoint {endpoint} is not canonical"
                ));
            }
            if !seen.insert(*endpoint) {
                return Err(format!(
                    "duplicate configured cluster backend endpoint {endpoint}"
                ));
            }
        }
        Ok(Self {
            role,
            seed_endpoints,
            heartbeat_interval,
            heartbeat_timeout_retries,
            decommission_timeout,
        })
    }

    pub const fn role(&self) -> ClusterRole {
        self.role
    }

    pub fn seed_endpoints(&self) -> &[SocketAddr] {
        &self.seed_endpoints
    }

    pub const fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }

    pub const fn heartbeat_timeout_retries(&self) -> u32 {
        self.heartbeat_timeout_retries
    }

    pub const fn decommission_timeout(&self) -> Duration {
        self.decommission_timeout
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DesiredBackendState {
    Active,
    Decommissioning,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredClusterBackendEntryV1 {
    backend_id: u32,
    endpoint: String,
    desired_state: DesiredBackendState,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredClusterBackendsV1 {
    schema_version: u8,
    last_operation_id: Uuid,
    next_backend_id: u64,
    entries: Vec<StoredClusterBackendEntryV1>,
}

impl Default for StoredClusterBackendsV1 {
    fn default() -> Self {
        Self {
            schema_version: CLUSTER_BACKENDS_SCHEMA_VERSION,
            last_operation_id: Uuid::nil(),
            next_backend_id: 0,
            entries: Vec::new(),
        }
    }
}

#[derive(Clone, Debug)]
enum RepositoryMutation {
    ReconcileSeeds(Vec<SocketAddr>),
    Add(SocketAddr),
    MarkDecommissioning {
        backend_id: u32,
        endpoint: SocketAddr,
    },
    Remove {
        backend_id: u32,
        endpoint: SocketAddr,
    },
}

impl RepositoryMutation {
    fn apply(&self, state: &mut StoredClusterBackendsV1) -> Result<bool, String> {
        validate_stored_cluster_backends(state)?;
        match self {
            Self::ReconcileSeeds(seeds) => {
                let mut changed = false;
                for endpoint in seeds {
                    if state
                        .entries
                        .iter()
                        .any(|entry| entry.endpoint == endpoint.to_string())
                    {
                        continue;
                    }
                    let backend_id = allocate_backend_id(state)?;
                    state.entries.push(StoredClusterBackendEntryV1 {
                        backend_id,
                        endpoint: endpoint.to_string(),
                        desired_state: DesiredBackendState::Active,
                    });
                    changed = true;
                }
                Ok(changed)
            }
            Self::Add(endpoint) => {
                if state
                    .entries
                    .iter()
                    .any(|entry| entry.endpoint == endpoint.to_string())
                {
                    return Ok(false);
                }
                let backend_id = allocate_backend_id(state)?;
                state.entries.push(StoredClusterBackendEntryV1 {
                    backend_id,
                    endpoint: endpoint.to_string(),
                    desired_state: DesiredBackendState::Active,
                });
                Ok(true)
            }
            Self::MarkDecommissioning {
                backend_id,
                endpoint,
            } => {
                let entry = stored_entry_mut(state, *backend_id, *endpoint)?;
                if entry.desired_state == DesiredBackendState::Decommissioning {
                    Ok(false)
                } else {
                    entry.desired_state = DesiredBackendState::Decommissioning;
                    Ok(true)
                }
            }
            Self::Remove {
                backend_id,
                endpoint,
            } => {
                let index = state
                    .entries
                    .iter()
                    .position(|entry| {
                        entry.backend_id == *backend_id && entry.endpoint == endpoint.to_string()
                    })
                    .ok_or_else(|| {
                        format!(
                            "backend {backend_id} at {endpoint} is absent from durable membership"
                        )
                    })?;
                state.entries.remove(index);
                Ok(true)
            }
        }
    }

    fn matches_postcondition(&self, state: &StoredClusterBackendsV1) -> bool {
        match self {
            Self::ReconcileSeeds(seeds) => seeds.iter().all(|endpoint| {
                state
                    .entries
                    .iter()
                    .any(|entry| entry.endpoint == endpoint.to_string())
            }),
            Self::Add(endpoint) => state
                .entries
                .iter()
                .any(|entry| entry.endpoint == endpoint.to_string()),
            Self::MarkDecommissioning {
                backend_id,
                endpoint,
            } => state.entries.iter().any(|entry| {
                entry.backend_id == *backend_id
                    && entry.endpoint == endpoint.to_string()
                    && entry.desired_state == DesiredBackendState::Decommissioning
            }),
            Self::Remove {
                backend_id,
                endpoint,
            } => !state.entries.iter().any(|entry| {
                entry.backend_id == *backend_id && entry.endpoint == endpoint.to_string()
            }),
        }
    }
}

#[derive(Clone)]
struct ClusterBackendRepository {
    store: Arc<dyn StateStore>,
    metrics: Arc<novarocks_state_store::metrics::StateStoreMetrics>,
}

impl ClusterBackendRepository {
    fn new(store: Arc<dyn StateStore>) -> Self {
        let metrics = Arc::new(novarocks_state_store::metrics::StateStoreMetrics::new(
            store.metrics_snapshot().provider,
        ));
        Self { store, metrics }
    }

    async fn load(&self) -> Result<Option<StoredClusterBackendsV1>, String> {
        let key = cluster_backends_key()?;
        let mut transaction =
            self.store.begin_read().await.map_err(|error| {
                format!("begin cluster backend membership read failed: {error}")
            })?;
        let record = transaction
            .get(&key)
            .await
            .map_err(|error| format!("read cluster backend membership failed: {error}"))?;
        transaction
            .abort()
            .await
            .map_err(|error| format!("finish cluster backend membership read failed: {error}"))?;
        record
            .map(decode_stored_cluster_backends)
            .transpose()
            .map_err(|error| format!("decode cluster backend membership failed: {error}"))
    }

    async fn mutate(
        &self,
        mutation: RepositoryMutation,
    ) -> Result<StoredClusterBackendsV1, String> {
        let operation_id = OperationId::new_v7();
        let key = cluster_backends_key()?;
        let result = run_side_effect_free(
            self.store.as_ref(),
            self.metrics.as_ref(),
            operation_id,
            "mutate frontend cluster backend membership",
            |transaction| {
                let key = key.clone();
                let mutation = mutation.clone();
                Box::pin(async move {
                    let existing = transaction.get(&key).await?;
                    let (mut state, precondition) = match existing {
                        Some(record) => {
                            let version = record.version.clone();
                            (
                                decode_stored_cluster_backends(record)?,
                                Precondition::Version(version),
                            )
                        }
                        None => (StoredClusterBackendsV1::default(), Precondition::Absent),
                    };
                    let changed = mutation
                        .apply(&mut state)
                        .map_err(invalid_state_store_request)?;
                    if !changed {
                        return Ok(state);
                    }
                    state.last_operation_id = *operation_id.as_uuid();
                    validate_stored_cluster_backends(&state)
                        .map_err(invalid_state_store_request)?;
                    transaction
                        .put(key, encode_stored_cluster_backends(&state)?, precondition)
                        .await?;
                    Ok(state)
                })
            },
        )
        .await;
        match result {
            Ok(success) => Ok(success.value),
            Err(RunFailure::CommitUnknown {
                transaction_id,
                error,
            }) => {
                self.resolve_commit_unknown(transaction_id, operation_id, &mutation, error)
                    .await
            }
            Err(error) => Err(format!(
                "cluster backend membership mutation failed: {error:?}"
            )),
        }
    }

    async fn resolve_commit_unknown(
        &self,
        transaction_id: novarocks_spi::state_store::TransactionId,
        operation_id: OperationId,
        mutation: &RepositoryMutation,
        original_error: novarocks_spi::state_store::StateStoreError,
    ) -> Result<StoredClusterBackendsV1, String> {
        match self.store.resolve_commit(&transaction_id).await {
            Ok(CommitResolution::Committed(receipt)) => {
                if receipt.transaction_id != transaction_id {
                    return Err(
                        "cluster backend commit resolution receipt has a mismatched transaction id"
                            .to_string(),
                    );
                }
                self.authoritative_committed_state(operation_id, mutation, original_error)
                    .await
            }
            Ok(CommitResolution::NotCommitted) => Err(format!(
                "cluster backend membership mutation definitely did not commit: {original_error}"
            )),
            Ok(CommitResolution::Unresolved) | Err(_) => {
                self.authoritative_committed_state(operation_id, mutation, original_error)
                    .await
            }
        }
    }

    async fn authoritative_committed_state(
        &self,
        operation_id: OperationId,
        mutation: &RepositoryMutation,
        original_error: novarocks_spi::state_store::StateStoreError,
    ) -> Result<StoredClusterBackendsV1, String> {
        match self.load().await? {
            Some(state)
                if state.last_operation_id == *operation_id.as_uuid()
                    && mutation.matches_postcondition(&state) =>
            {
                Ok(state)
            }
            Some(state) if state.last_operation_id == *operation_id.as_uuid() => Err(
                "cluster backend membership corruption: committed operation has an invalid postcondition"
                    .to_string(),
            ),
            _ => Err(format!(
                "cluster backend membership commit outcome is unresolved: {original_error}"
            )),
        }
    }
}

#[derive(Clone)]
enum MembershipStorage {
    Durable(ClusterBackendRepository),
    Transient,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RuntimeBackendState {
    Registering,
    Live,
    Lost,
    Decommissioning,
}

impl RuntimeBackendState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Registering => "Registering",
            Self::Live => "Live",
            Self::Lost => "Lost",
            Self::Decommissioning => "Decommissioning",
        }
    }
}

#[derive(Clone, Debug)]
struct FrontendBackendEntry {
    endpoint: SocketAddr,
    state: RuntimeBackendState,
    start_epoch: u64,
    version: String,
    num_cores: u32,
    last_heartbeat_ms: i64,
    missed_heartbeats: u32,
    scheduled_fragments: u64,
    last_err: Option<String>,
    decommission_started: Option<Instant>,
    decommission_timeout_event_sent: bool,
}

struct TopologyState {
    timeout_retries: u32,
    decommission_timeout: Duration,
    topology_revision: u64,
    terminal_error: Option<String>,
    entries: BTreeMap<usize, FrontendBackendEntry>,
}

#[derive(Clone, Copy)]
struct HeartbeatSignal {
    generation: u64,
    stopping: bool,
}

/// The sole frontend membership owner. Durable desired membership is kept in
/// StateStore; liveness and fragment activity remain process-local observations.
pub(crate) struct ClusterBackendService {
    state: Mutex<TopologyState>,
    storage: MembershipStorage,
    runtime: Handle,
    heartbeat_interval: Duration,
    mutation: Mutex<()>,
    query_events: Mutex<Option<Arc<dyn BackendQueryEventSink>>>,
    heartbeat_probe: Arc<HeartbeatProbe>,
    heartbeat_thread: Mutex<Option<JoinHandle<()>>>,
    heartbeat_round: Mutex<()>,
    heartbeat_signal: Mutex<HeartbeatSignal>,
    heartbeat_wake: Condvar,
}

impl ClusterBackendService {
    pub(crate) async fn open(
        config: ClusterBackendOpenConfig,
        state_store: Option<Arc<dyn StateStore>>,
        runtime: Handle,
    ) -> Result<Arc<Self>, String> {
        let storage = match config.role() {
            ClusterRole::Fe => MembershipStorage::Durable(ClusterBackendRepository::new(
                state_store.ok_or_else(|| {
                    "role=fe requires StateStore for durable cluster backend membership".to_string()
                })?,
            )),
            ClusterRole::AllInOne => MembershipStorage::Transient,
            ClusterRole::Be => {
                return Err("role=be must not open ClusterBackendService".to_string());
            }
        };
        let service = Arc::new(Self::new(storage, runtime, &config, |be_id, endpoint| {
            novarocks::service::cluster_heartbeat::grpc_heartbeat(be_id, endpoint)
        }));
        if let MembershipStorage::Durable(repository) = &service.storage {
            let stored = match repository.load().await? {
                Some(_) => {
                    repository
                        .mutate(RepositoryMutation::ReconcileSeeds(
                            config.seed_endpoints.clone(),
                        ))
                        .await?
                }
                None if config.seed_endpoints.is_empty() => StoredClusterBackendsV1::default(),
                None => {
                    repository
                        .mutate(RepositoryMutation::ReconcileSeeds(
                            config.seed_endpoints.clone(),
                        ))
                        .await?
                }
            };
            service.restore_durable_state(stored)?;
        } else {
            for endpoint in config.seed_endpoints() {
                service.add_transient(*endpoint)?;
            }
        }
        service.publish_snapshot();
        Ok(service)
    }

    fn new<F>(
        storage: MembershipStorage,
        runtime: Handle,
        config: &ClusterBackendOpenConfig,
        heartbeat_probe: F,
    ) -> Self
    where
        F: Fn(u32, SocketAddr) -> HeartbeatOutcome + Send + Sync + 'static,
    {
        Self {
            state: Mutex::new(TopologyState {
                timeout_retries: config.heartbeat_timeout_retries,
                decommission_timeout: config.decommission_timeout,
                topology_revision: 0,
                terminal_error: None,
                entries: BTreeMap::new(),
            }),
            storage,
            runtime,
            heartbeat_interval: config.heartbeat_interval,
            mutation: Mutex::new(()),
            query_events: Mutex::new(None),
            heartbeat_probe: Arc::new(heartbeat_probe),
            heartbeat_thread: Mutex::new(None),
            heartbeat_round: Mutex::new(()),
            heartbeat_signal: Mutex::new(HeartbeatSignal {
                generation: 0,
                stopping: false,
            }),
            heartbeat_wake: Condvar::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_transient_for_test(timeout_retries: u32) -> Self {
        let config = ClusterBackendOpenConfig::new(
            ClusterRole::AllInOne,
            Vec::new(),
            Duration::from_millis(1),
            timeout_retries.max(1),
            Duration::from_secs(1),
        )
        .expect("valid test topology configuration");
        let runtime = Handle::try_current().unwrap_or_else(|_| {
            novarocks::runtime::global_async_runtime::data_runtime_handle()
                .expect("initialize data runtime for topology test")
        });
        Self::new(
            MembershipStorage::Transient,
            runtime,
            &config,
            |be_id, endpoint| {
                novarocks::service::cluster_heartbeat::grpc_heartbeat(be_id, endpoint)
            },
        )
    }

    #[cfg(test)]
    pub(crate) fn from_captured_targets_for_test(targets: &[LiveBackendTarget]) -> Self {
        let service = Self::new_transient_for_test(1);
        let mut state = service.state.lock().expect("frontend topology lock");
        state.entries = targets
            .iter()
            .map(|target| {
                (
                    target.backend_idx(),
                    FrontendBackendEntry {
                        endpoint: target.endpoint(),
                        state: RuntimeBackendState::Live,
                        start_epoch: target.start_epoch(),
                        version: String::new(),
                        num_cores: 0,
                        last_heartbeat_ms: 0,
                        missed_heartbeats: 0,
                        scheduled_fragments: 0,
                        last_err: None,
                        decommission_started: None,
                        decommission_timeout_event_sent: false,
                    },
                )
            })
            .collect();
        drop(state);
        service
    }

    fn restore_durable_state(&self, stored: StoredClusterBackendsV1) -> Result<(), String> {
        validate_stored_cluster_backends(&stored)?;
        let mut entries = BTreeMap::new();
        for entry in stored.entries {
            let endpoint = parse_canonical_endpoint(&entry.endpoint)?;
            let backend_idx = usize::try_from(entry.backend_id).map_err(|_| {
                format!(
                    "backend id {} cannot be represented locally",
                    entry.backend_id
                )
            })?;
            entries.insert(
                backend_idx,
                FrontendBackendEntry {
                    endpoint,
                    state: match entry.desired_state {
                        DesiredBackendState::Active => RuntimeBackendState::Registering,
                        DesiredBackendState::Decommissioning => {
                            RuntimeBackendState::Decommissioning
                        }
                    },
                    start_epoch: 0,
                    version: String::new(),
                    num_cores: 0,
                    last_heartbeat_ms: 0,
                    missed_heartbeats: 0,
                    scheduled_fragments: 0,
                    last_err: None,
                    decommission_started: (entry.desired_state
                        == DesiredBackendState::Decommissioning)
                        .then(Instant::now),
                    decommission_timeout_event_sent: false,
                },
            );
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| "lock frontend topology failed".to_string())?;
        state.entries = entries;
        if !state.entries.is_empty() {
            advance_topology_revision(&mut state).map_err(|error| error.to_string())?;
        }
        Ok(())
    }

    pub(crate) fn attach_query_events(&self, events: Arc<dyn BackendQueryEventSink>) {
        *self
            .query_events
            .lock()
            .expect("frontend topology event sink lock") = Some(events);
        self.publish_snapshot();
    }

    pub(crate) fn detach_query_events(&self) {
        self.query_events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
    }

    pub(crate) fn start_heartbeat_manager(self: &Arc<Self>) -> Result<(), String> {
        let mut heartbeat_thread = self
            .heartbeat_thread
            .lock()
            .map_err(|_| "lock frontend topology heartbeat thread failed".to_string())?;
        if heartbeat_thread.is_some() {
            return Ok(());
        }
        self.heartbeat_signal
            .lock()
            .map_err(|_| "lock frontend topology heartbeat signal failed".to_string())?
            .stopping = false;
        let service = Arc::clone(self);
        let interval = self.heartbeat_interval();
        *heartbeat_thread = Some(
            std::thread::Builder::new()
                .name("frontend-heartbeat-manager".to_string())
                .spawn(move || service.run_heartbeat_manager(interval))
                .map_err(|error| format!("spawn frontend heartbeat manager failed: {error}"))?,
        );
        Ok(())
    }

    pub(crate) fn stop_heartbeat_manager(&self) -> Result<(), String> {
        {
            let mut signal = self
                .heartbeat_signal
                .lock()
                .map_err(|_| "lock frontend topology heartbeat signal failed".to_string())?;
            signal.stopping = true;
            signal.generation = signal.generation.wrapping_add(1);
        }
        self.heartbeat_wake.notify_all();
        let join = self
            .heartbeat_thread
            .lock()
            .map_err(|_| "lock frontend topology heartbeat thread failed".to_string())?
            .take();
        if let Some(join) = join {
            join.join()
                .map_err(|payload| format!("frontend heartbeat manager panicked: {payload:?}"))?;
        }
        self.heartbeat_signal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .stopping = false;
        Ok(())
    }

    fn run_heartbeat_manager(&self, interval: Duration) {
        let mut observed_generation = 0;
        loop {
            if self.topology_terminal_error().is_some() || self.heartbeat_is_stopping() {
                return;
            }
            {
                let _round = self
                    .heartbeat_round
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                self.process_decommissioning_once();
                for (backend_idx, entry) in self.heartbeat_rows() {
                    if self.heartbeat_is_stopping() {
                        return;
                    }
                    let Ok(be_id) = u32::try_from(backend_idx) else {
                        continue;
                    };
                    match (self.heartbeat_probe)(be_id, entry.endpoint) {
                        HeartbeatOutcome::Ok {
                            start_epoch,
                            version,
                            num_cores,
                            now_ms,
                        } => self.record_heartbeat_success(
                            backend_idx,
                            start_epoch,
                            version,
                            num_cores,
                            now_ms,
                        ),
                        HeartbeatOutcome::Failed { err } => {
                            self.record_heartbeat_failure_with_error(backend_idx, err);
                        }
                    }
                }
            }
            let signal = self
                .heartbeat_signal
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if signal.stopping {
                return;
            }
            let signal = if signal.generation == observed_generation {
                self.heartbeat_wake
                    .wait_timeout_while(signal, interval, |signal| {
                        !signal.stopping && signal.generation == observed_generation
                    })
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .0
            } else {
                signal
            };
            if signal.stopping {
                return;
            }
            observed_generation = signal.generation;
        }
    }

    fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }

    fn heartbeat_is_stopping(&self) -> bool {
        self.heartbeat_signal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .stopping
    }

    fn wake_heartbeat_manager(&self) {
        let mut signal = self
            .heartbeat_signal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        signal.generation = signal.generation.wrapping_add(1);
        drop(signal);
        self.heartbeat_wake.notify_all();
    }

    pub(crate) fn record_heartbeat_success(
        &self,
        backend_idx: usize,
        start_epoch: u64,
        version: impl Into<String>,
        num_cores: u32,
        now_ms: i64,
    ) {
        let mut state = self.state.lock().expect("frontend topology lock");
        let Some(entry) = state.entries.get_mut(&backend_idx) else {
            return;
        };
        if entry.state == RuntimeBackendState::Decommissioning {
            return;
        }
        let old_state = entry.state;
        let old_epoch = entry.start_epoch;
        entry.state = RuntimeBackendState::Live;
        entry.start_epoch = start_epoch;
        entry.version = version.into();
        entry.num_cores = num_cores;
        entry.last_heartbeat_ms = now_ms;
        entry.missed_heartbeats = 0;
        entry.last_err = None;
        let restarted = (old_epoch != 0 && start_epoch != 0 && old_epoch != start_epoch).then_some(
            BackendQueryEvent::Restarted {
                backend_idx,
                old_epoch,
                new_epoch: start_epoch,
            },
        );
        if old_state != RuntimeBackendState::Live || old_epoch != start_epoch {
            if advance_topology_revision(&mut state).is_err() {
                return;
            }
        }
        drop(state);
        if let Some(event) = restarted {
            self.dispatch_event(event);
        }
        self.publish_snapshot();
    }

    #[cfg(test)]
    pub(crate) fn record_heartbeat_failure(&self, backend_idx: usize) -> bool {
        self.record_heartbeat_failure_with_error(backend_idx, "heartbeat failed")
    }

    fn record_heartbeat_failure_with_error(
        &self,
        backend_idx: usize,
        error: impl Into<String>,
    ) -> bool {
        let mut state = self.state.lock().expect("frontend topology lock");
        let timeout_retries = state.timeout_retries;
        let Some(entry) = state.entries.get_mut(&backend_idx) else {
            return false;
        };
        if entry.state == RuntimeBackendState::Decommissioning {
            return false;
        }
        entry.missed_heartbeats = entry.missed_heartbeats.saturating_add(1);
        entry.last_err = Some(error.into());
        let transitioned =
            entry.state != RuntimeBackendState::Lost && entry.missed_heartbeats >= timeout_retries;
        if transitioned {
            entry.state = RuntimeBackendState::Lost;
        }
        if transitioned && advance_topology_revision(&mut state).is_err() {
            return false;
        }
        drop(state);
        if transitioned {
            self.dispatch_event(BackendQueryEvent::Unavailable {
                backend_idx,
                reason: format!("backend {backend_idx} lost after heartbeat timeout"),
            });
            self.publish_snapshot();
        }
        transitioned
    }

    fn add_transient(&self, endpoint: SocketAddr) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "lock frontend topology failed".to_string())?;
        if state
            .entries
            .values()
            .any(|entry| entry.endpoint == endpoint)
        {
            return Ok(());
        }
        let backend_idx = next_runtime_backend_id(&state)?;
        advance_topology_revision(&mut state).map_err(|error| error.to_string())?;
        state
            .entries
            .insert(backend_idx, registering_entry(endpoint));
        drop(state);
        self.publish_snapshot();
        Ok(())
    }

    fn block_on<T>(&self, future: impl Future<Output = Result<T, String>>) -> Result<T, String> {
        match Handle::try_current() {
            Ok(_) if self.runtime.runtime_flavor() == RuntimeFlavor::CurrentThread => Err(
                "cluster backend membership cannot synchronously use StateStore from a current-thread Tokio runtime"
                    .to_string(),
            ),
            Ok(_) => tokio::task::block_in_place(|| self.runtime.block_on(future)),
            Err(_) => self.runtime.block_on(future),
        }
    }

    fn persist(
        &self,
        mutation: RepositoryMutation,
    ) -> Result<Option<StoredClusterBackendsV1>, String> {
        match &self.storage {
            MembershipStorage::Durable(repository) => {
                self.block_on(repository.mutate(mutation)).map(Some)
            }
            MembershipStorage::Transient => Ok(None),
        }
    }

    fn apply_added(
        &self,
        stored: Option<StoredClusterBackendsV1>,
        endpoint: SocketAddr,
    ) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "lock frontend topology failed".to_string())?;
        if state
            .entries
            .values()
            .any(|entry| entry.endpoint == endpoint)
        {
            return Ok(());
        }
        let backend_idx = match stored {
            Some(stored) => stored
                .entries
                .iter()
                .find(|entry| entry.endpoint == endpoint.to_string())
                .map(|entry| {
                    usize::try_from(entry.backend_id)
                        .map_err(|_| "backend id cannot be represented locally".to_string())
                })
                .transpose()?
                .ok_or_else(|| {
                    format!("durable membership did not contain added backend {endpoint}")
                })?,
            None => next_runtime_backend_id(&state)?,
        };
        advance_topology_revision(&mut state).map_err(|error| error.to_string())?;
        state
            .entries
            .insert(backend_idx, registering_entry(endpoint));
        drop(state);
        self.publish_snapshot();
        self.wake_heartbeat_manager();
        Ok(())
    }

    fn mark_decommissioning_runtime(
        &self,
        backend_idx: usize,
        endpoint: SocketAddr,
    ) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "lock frontend topology failed".to_string())?;
        let entry = state
            .entries
            .get_mut(&backend_idx)
            .ok_or_else(|| format!("backend {endpoint} not found"))?;
        if entry.endpoint != endpoint {
            return Err(format!("backend {backend_idx} identity changed"));
        }
        if entry.state != RuntimeBackendState::Decommissioning {
            entry.state = RuntimeBackendState::Decommissioning;
            entry.decommission_started = Some(Instant::now());
            entry.decommission_timeout_event_sent = false;
            advance_topology_revision(&mut state).map_err(|error| error.to_string())?;
        }
        drop(state);
        self.publish_snapshot();
        self.wake_heartbeat_manager();
        Ok(())
    }

    fn remove_runtime(&self, backend_idx: usize, endpoint: SocketAddr) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "lock frontend topology failed".to_string())?;
        if !state
            .entries
            .get(&backend_idx)
            .is_some_and(|entry| entry.endpoint == endpoint)
        {
            return Ok(());
        }
        advance_topology_revision(&mut state).map_err(|error| error.to_string())?;
        state.entries.remove(&backend_idx);
        drop(state);
        self.publish_snapshot();
        Ok(())
    }

    fn process_decommissioning_once(&self) {
        let candidates = self
            .rows()
            .into_iter()
            .filter(|(_, entry)| entry.state == RuntimeBackendState::Decommissioning)
            .collect::<Vec<_>>();
        for (backend_idx, entry) in candidates {
            if self.backend_has_active_queries(backend_idx) {
                let timed_out = entry
                    .decommission_started
                    .is_some_and(|started| started.elapsed() >= self.decommission_timeout());
                if timed_out && !entry.decommission_timeout_event_sent {
                    if let Ok(mut state) = self.state.lock() {
                        if let Some(current) = state.entries.get_mut(&backend_idx) {
                            current.decommission_timeout_event_sent = true;
                        }
                    }
                    self.dispatch_event(BackendQueryEvent::Unavailable {
                        backend_idx,
                        reason: format!("backend {backend_idx} decommission timed out"),
                    });
                }
                continue;
            }
            let _guard = self
                .mutation
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if self
                .persist(RepositoryMutation::Remove {
                    backend_id: backend_idx as u32,
                    endpoint: entry.endpoint,
                })
                .is_ok()
            {
                let _ = self.remove_runtime(backend_idx, entry.endpoint);
            }
        }
    }

    fn rows(&self) -> Vec<(usize, FrontendBackendEntry)> {
        self.state
            .lock()
            .expect("frontend topology lock")
            .entries
            .iter()
            .map(|(id, entry)| (*id, entry.clone()))
            .collect()
    }

    fn heartbeat_rows(&self) -> Vec<(usize, FrontendBackendEntry)> {
        self.rows()
            .into_iter()
            .filter(|(_, entry)| entry.state != RuntimeBackendState::Decommissioning)
            .collect()
    }

    fn topology_terminal_error(&self) -> Option<String> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .terminal_error
            .clone()
    }

    fn decommission_timeout(&self) -> Duration {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .decommission_timeout
    }

    fn publish_snapshot(&self) {
        let (revision, live, metrics) = {
            let state = self.state.lock().expect("frontend topology lock");
            let live =
                state
                    .entries
                    .iter()
                    .filter_map(|(id, entry)| {
                        (entry.state == RuntimeBackendState::Live).then_some(
                            LiveBackendTarget::new(*id, entry.endpoint, entry.start_epoch),
                        )
                    })
                    .collect();
            let mut metrics = BackendTopologyMetricsSnapshot::default();
            for entry in state.entries.values() {
                match entry.state {
                    RuntimeBackendState::Registering => metrics.registering += 1,
                    RuntimeBackendState::Live => metrics.live += 1,
                    RuntimeBackendState::Lost => metrics.lost += 1,
                    RuntimeBackendState::Decommissioning => metrics.decommissioning += 1,
                }
            }
            (state.topology_revision, live, metrics)
        };
        publish_backend_topology_metrics(metrics);
        if let Some(events) = self
            .query_events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
        {
            let _ = catch_unwind(AssertUnwindSafe(|| {
                events.replace_live_backends(revision, live)
            }));
        }
    }

    fn dispatch_event(&self, event: BackendQueryEvent) {
        if let Some(events) = self
            .query_events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
        {
            let _ = catch_unwind(AssertUnwindSafe(|| events.on_backend_event(event)));
        }
    }

    fn backend_has_active_queries(&self, backend_idx: usize) -> bool {
        self.query_events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .is_some_and(|events| {
                catch_unwind(AssertUnwindSafe(|| {
                    events.backend_has_active_queries(backend_idx)
                }))
                .unwrap_or(true)
            })
    }

    #[cfg(test)]
    fn live_backends(&self) -> Vec<LiveBackendTarget> {
        self.snapshot()
            .expect("test topology snapshot is available")
            .targets()
            .to_vec()
    }

    #[cfg(test)]
    pub(crate) fn backend_count_for_test(&self) -> usize {
        self.state
            .lock()
            .expect("frontend topology lock")
            .entries
            .len()
    }

    #[cfg(test)]
    pub(crate) fn scheduled_fragment_count_for_test(&self, backend_idx: usize) -> u64 {
        self.state
            .lock()
            .expect("frontend topology lock")
            .entries
            .get(&backend_idx)
            .map_or(0, |entry| entry.scheduled_fragments)
    }
}

impl BackendTopologyPort for ClusterBackendService {
    fn snapshot(&self) -> Result<BackendTopologySnapshot, BackendTopologyError> {
        let state = self
            .state
            .lock()
            .map_err(|_| BackendTopologyError::Unavailable {
                message: "frontend topology lock is poisoned".to_string(),
            })?;
        if let Some(message) = &state.terminal_error {
            return Err(BackendTopologyError::Unavailable {
                message: message.clone(),
            });
        }
        BackendTopologySnapshot::try_new(
            state.topology_revision,
            state
                .entries
                .iter()
                .filter_map(|(id, entry)| {
                    (entry.state == RuntimeBackendState::Live).then_some(LiveBackendTarget::new(
                        *id,
                        entry.endpoint,
                        entry.start_epoch,
                    ))
                })
                .collect(),
        )
    }

    fn validate_snapshot(
        &self,
        expected: &BackendTopologySnapshot,
    ) -> Result<(), BackendTopologyValidationError> {
        let current = self
            .snapshot()
            .map_err(BackendTopologyValidationError::Unavailable)?;
        if current.revision() != expected.revision() {
            return Err(BackendTopologyValidationError::RevisionChanged {
                captured_revision: expected.revision(),
                current_revision: current.revision(),
            });
        }
        if current == *expected {
            return Ok(());
        }
        for captured in expected.targets() {
            match current.target(captured.backend_idx()) {
                None => {
                    return Err(BackendTopologyValidationError::TargetMissing {
                        backend_idx: captured.backend_idx(),
                        captured_generation: captured.start_epoch(),
                        captured_revision: expected.revision(),
                        current_revision: current.revision(),
                    });
                }
                Some(current_target) if current_target.start_epoch() != captured.start_epoch() => {
                    return Err(BackendTopologyValidationError::GenerationChanged {
                        backend_idx: captured.backend_idx(),
                        captured_generation: captured.start_epoch(),
                        current_generation: current_target.start_epoch(),
                        captured_revision: expected.revision(),
                        current_revision: current.revision(),
                    });
                }
                Some(_) => {}
            }
        }
        Err(
            BackendTopologyValidationError::ContentChangedWithoutRevision {
                revision: expected.revision(),
            },
        )
    }

    fn record_successful_fragment_submission(&self, backend_idx: usize) {
        if let Some(entry) = self
            .state
            .lock()
            .expect("frontend topology lock")
            .entries
            .get_mut(&backend_idx)
        {
            entry.scheduled_fragments = entry.scheduled_fragments.saturating_add(1);
        }
    }

    fn add_backend(&self, endpoint: SocketAddr) -> Result<(), String> {
        let _guard = self
            .mutation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self
            .rows()
            .iter()
            .any(|(_, entry)| entry.endpoint == endpoint)
        {
            return Ok(());
        }
        let stored = self.persist(RepositoryMutation::Add(endpoint))?;
        self.apply_added(stored, endpoint)
    }

    fn drop_backend(&self, endpoint: SocketAddr, force: bool) -> Result<(), String> {
        let _guard = self
            .mutation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let backend_idx = self
            .rows()
            .iter()
            .find_map(|(id, entry)| (entry.endpoint == endpoint).then_some(*id))
            .ok_or_else(|| format!("backend {endpoint} not found"))?;
        let backend_id = u32::try_from(backend_idx)
            .map_err(|_| format!("backend id {backend_idx} is outside durable range"))?;
        if force {
            self.persist(RepositoryMutation::Remove {
                backend_id,
                endpoint,
            })?;
            self.remove_runtime(backend_idx, endpoint)?;
            self.dispatch_event(BackendQueryEvent::Unavailable {
                backend_idx,
                reason: format!("backend {backend_idx} dropped forcefully"),
            });
            return Ok(());
        }
        self.persist(RepositoryMutation::MarkDecommissioning {
            backend_id,
            endpoint,
        })?;
        self.mark_decommissioning_runtime(backend_idx, endpoint)?;
        if !self.backend_has_active_queries(backend_idx) {
            self.persist(RepositoryMutation::Remove {
                backend_id,
                endpoint,
            })?;
            self.remove_runtime(backend_idx, endpoint)?;
        }
        Ok(())
    }

    fn show_backends(&self) -> Result<QueryResult, String> {
        let names = [
            "BackendId",
            "Host",
            "GrpcPort",
            "State",
            "ScheduledFragments",
        ];
        let mut columns = vec![Vec::<String>::new(); names.len()];
        for (backend_idx, entry) in self.rows() {
            columns[0].push(backend_idx.to_string());
            columns[1].push(entry.endpoint.ip().to_string());
            columns[2].push(entry.endpoint.port().to_string());
            columns[3].push(entry.state.as_str().to_string());
            columns[4].push(entry.scheduled_fragments.to_string());
        }
        let arrays = columns
            .into_iter()
            .map(|values| Arc::new(StringArray::from(values)) as Arc<dyn arrow::array::Array>)
            .collect();
        let schema = Schema::new(
            names
                .iter()
                .map(|name| Field::new(*name, DataType::Utf8, false))
                .collect::<Vec<_>>(),
        );
        let batch = RecordBatch::try_new(Arc::new(schema), arrays)
            .map_err(|error| format!("build SHOW BACKENDS result failed: {error}"))?;
        Ok(QueryResult {
            columns: names
                .iter()
                .map(|name| QueryResultColumn {
                    name: (*name).to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    logical_type: None,
                })
                .collect(),
            chunks: vec![record_batch_to_chunk(batch)?],
        })
    }
}

fn registering_entry(endpoint: SocketAddr) -> FrontendBackendEntry {
    FrontendBackendEntry {
        endpoint,
        state: RuntimeBackendState::Registering,
        start_epoch: 0,
        version: String::new(),
        num_cores: 0,
        last_heartbeat_ms: 0,
        missed_heartbeats: 0,
        scheduled_fragments: 0,
        last_err: None,
        decommission_started: None,
        decommission_timeout_event_sent: false,
    }
}

fn next_runtime_backend_id(state: &TopologyState) -> Result<usize, String> {
    state
        .entries
        .keys()
        .next_back()
        .copied()
        .map(|id| {
            id.checked_add(1)
                .ok_or_else(|| "frontend backend id overflow".to_string())
        })
        .transpose()
        .map(|id| id.unwrap_or(0))
}

fn advance_topology_revision(state: &mut TopologyState) -> Result<(), BackendTopologyError> {
    if let Some(message) = &state.terminal_error {
        return Err(BackendTopologyError::Unavailable {
            message: message.clone(),
        });
    }
    state.topology_revision = state.topology_revision.checked_add(1).ok_or_else(|| {
        let message = "frontend topology revision space is exhausted".to_string();
        state.terminal_error = Some(message);
        BackendTopologyError::RevisionExhausted
    })?;
    Ok(())
}

fn cluster_backends_key() -> Result<Key, String> {
    Key::try_from(Bytes::from_static(CLUSTER_BACKENDS_KEY))
        .map_err(|error| format!("build cluster backend membership key failed: {error}"))
}

fn encode_stored_cluster_backends(
    state: &StoredClusterBackendsV1,
) -> Result<Value, novarocks_spi::state_store::StateStoreError> {
    let bytes = serde_json::to_vec(state).map_err(|error| {
        invalid_state_store_request(format!("encode cluster backend membership failed: {error}"))
    })?;
    Value::try_from(Bytes::from(bytes))
}

fn decode_stored_cluster_backends(
    record: StateRecord,
) -> Result<StoredClusterBackendsV1, novarocks_spi::state_store::StateStoreError> {
    let key = cluster_backends_key().map_err(invalid_state_store_request)?;
    if record.key != key {
        return Err(invalid_state_store_request(
            "cluster backend membership record has an unexpected key",
        ));
    }
    let state = serde_json::from_slice(record.value.as_bytes()).map_err(|error| {
        invalid_state_store_request(format!("decode cluster backend membership failed: {error}"))
    })?;
    validate_stored_cluster_backends(&state).map_err(invalid_state_store_request)?;
    Ok(state)
}

fn validate_stored_cluster_backends(state: &StoredClusterBackendsV1) -> Result<(), String> {
    if state.schema_version != CLUSTER_BACKENDS_SCHEMA_VERSION {
        return Err(format!(
            "unsupported cluster backend membership schema version {}",
            state.schema_version
        ));
    }
    if state.next_backend_id > u64::from(u32::MAX) + 1 {
        return Err("cluster backend membership next id exceeds u32 range".to_string());
    }
    let mut previous = None;
    let mut endpoints = BTreeSet::new();
    for entry in &state.entries {
        if previous.is_some_and(|id| entry.backend_id <= id) {
            return Err("cluster backend membership ids are not strictly ordered".to_string());
        }
        if u64::from(entry.backend_id) >= state.next_backend_id {
            return Err(
                "cluster backend membership next id does not exceed all assigned ids".to_string(),
            );
        }
        parse_canonical_endpoint(&entry.endpoint)?;
        if !endpoints.insert(&entry.endpoint) {
            return Err(format!(
                "cluster backend membership has duplicate endpoint {}",
                entry.endpoint
            ));
        }
        previous = Some(entry.backend_id);
    }
    Ok(())
}

fn parse_canonical_endpoint(value: &str) -> Result<SocketAddr, String> {
    let endpoint = value
        .parse::<SocketAddr>()
        .map_err(|error| format!("invalid cluster backend endpoint {value:?}: {error}"))?;
    if endpoint.to_string() != value {
        return Err(format!(
            "cluster backend endpoint {value:?} is not canonical"
        ));
    }
    Ok(endpoint)
}

fn allocate_backend_id(state: &mut StoredClusterBackendsV1) -> Result<u32, String> {
    if state.next_backend_id > u64::from(u32::MAX) {
        return Err("cluster backend id space is exhausted".to_string());
    }
    let id = u32::try_from(state.next_backend_id)
        .map_err(|_| "cluster backend id exceeds u32 range".to_string())?;
    state.next_backend_id += 1;
    Ok(id)
}

fn stored_entry_mut(
    state: &mut StoredClusterBackendsV1,
    backend_id: u32,
    endpoint: SocketAddr,
) -> Result<&mut StoredClusterBackendEntryV1, String> {
    state
        .entries
        .iter_mut()
        .find(|entry| entry.backend_id == backend_id && entry.endpoint == endpoint.to_string())
        .ok_or_else(|| {
            format!("backend {backend_id} at {endpoint} is absent from durable membership")
        })
}

fn invalid_state_store_request(
    message: impl Into<String>,
) -> novarocks_spi::state_store::StateStoreError {
    let _ = message.into();
    novarocks_spi::state_store::StateStoreError::new(
        novarocks_spi::state_store::StateStoreErrorKind::InvalidRequest,
        "invalid cluster backend membership record",
    )
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use novarocks::query_execution::backend::{BackendTopologyPort, LiveBackendTarget};

    use super::{
        ClusterBackendOpenConfig, ClusterBackendService, DesiredBackendState,
        StoredClusterBackendEntryV1, StoredClusterBackendsV1, validate_stored_cluster_backends,
    };

    #[test]
    fn snapshot_and_management_are_frontend_owned() {
        let service = ClusterBackendService::new_transient_for_test(1);
        let endpoint: SocketAddr = "127.0.0.1:9070".parse().unwrap();
        service.add_backend(endpoint).unwrap();
        service.record_heartbeat_success(0, 17, "test", 2, 100);
        assert_eq!(
            service.live_backends(),
            [LiveBackendTarget::new(0, endpoint, 17)]
        );
        service.drop_backend(endpoint, true).unwrap();
        assert!(service.live_backends().is_empty());
    }

    #[test]
    fn stored_membership_rejects_unsorted_duplicate_and_noncanonical_entries() {
        let mut state = StoredClusterBackendsV1 {
            schema_version: 1,
            last_operation_id: uuid::Uuid::nil(),
            next_backend_id: 2,
            entries: vec![
                StoredClusterBackendEntryV1 {
                    backend_id: 1,
                    endpoint: "127.0.0.1:9001".to_string(),
                    desired_state: DesiredBackendState::Active,
                },
                StoredClusterBackendEntryV1 {
                    backend_id: 0,
                    endpoint: "127.0.0.1:9001".to_string(),
                    desired_state: DesiredBackendState::Active,
                },
            ],
        };
        assert!(validate_stored_cluster_backends(&state).is_err());
        state.entries.reverse();
        assert!(validate_stored_cluster_backends(&state).is_err());
    }

    #[test]
    fn open_config_rejects_duplicate_seed_identity() {
        let endpoint: SocketAddr = "127.0.0.1:9010".parse().unwrap();
        assert!(
            ClusterBackendOpenConfig::new(
                novarocks::common::app_config::ClusterRole::Fe,
                vec![endpoint, endpoint],
                std::time::Duration::from_secs(1),
                1,
                std::time::Duration::from_secs(1),
            )
            .is_err()
        );
    }
}
