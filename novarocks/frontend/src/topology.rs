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

//! FE-owned observed backend topology.
//!
//! BE announce is the only descriptor source.  FE pull heartbeats prove the
//! exact descriptor.  The registry stores orthogonal raw facts, and derives
//! eligibility from them; it has no durable membership catalogue or seed
//! reconciliation path.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_execution::task_execution::AdmissionEpochCapability;
use novarocks_proto_codec::membership::{BackendProcessDescriptor, BackendReportedState};
use novarocks_types::{BackendProcessId, ClusterRole, NativeCompatibilityId, NativeEndpoint};
use tokio::runtime::Handle;
use tokio::sync::watch;

use crate::common::backend_topology::{
    BackendProcessObservation, BackendProcessObservationPort, BackendTopologyError,
    BackendTopologyMetricsSnapshot, BackendTopologyPort, BackendTopologySnapshot,
    BackendTopologyValidationError, HeartbeatOutcome, LiveBackendTarget,
    publish_backend_topology_metrics,
};
use crate::metrics::{record_backend_announce, record_backend_heartbeat};
use crate::native::data_runtime::FrontendDataRuntime;
use crate::native::transport::heartbeat as native_heartbeat;
use crate::runtime::query_result::{QueryResult, QueryResultColumn, record_batch_to_chunk};

#[derive(Clone, Debug)]
pub struct ClusterBackendOpenConfig {
    role: ClusterRole,
    native_compatibility_id: NativeCompatibilityId,
    heartbeat_interval: Duration,
    heartbeat_timeout_retries: u32,
    announce_lease_ttl: Duration,
}

/// Sanitized compatibility-island facts read from the topology authority.
///
/// The topology lock supplies the revision and all counts as one observation.
/// Management owns how these facts are composed with FE-local serving state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BackendIslandSnapshot {
    native_compatibility_id: NativeCompatibilityId,
    topology_revision: u64,
    compatible_eligible_backend_count: usize,
    other_island_backend_count: usize,
    unknown_or_invalid_backend_count: usize,
}

impl BackendIslandSnapshot {
    pub(crate) const fn new(
        native_compatibility_id: NativeCompatibilityId,
        topology_revision: u64,
        compatible_eligible_backend_count: usize,
        other_island_backend_count: usize,
        unknown_or_invalid_backend_count: usize,
    ) -> Self {
        Self {
            native_compatibility_id,
            topology_revision,
            compatible_eligible_backend_count,
            other_island_backend_count,
            unknown_or_invalid_backend_count,
        }
    }

    pub const fn starting(native_compatibility_id: NativeCompatibilityId) -> Self {
        Self::new(native_compatibility_id, 0, 0, 0, 0)
    }

    pub const fn native_compatibility_id(&self) -> NativeCompatibilityId {
        self.native_compatibility_id
    }

    pub const fn topology_revision(&self) -> u64 {
        self.topology_revision
    }

    pub const fn compatible_eligible_backend_count(&self) -> usize {
        self.compatible_eligible_backend_count
    }

    pub const fn other_island_backend_count(&self) -> usize {
        self.other_island_backend_count
    }

    pub const fn unknown_or_invalid_backend_count(&self) -> usize {
        self.unknown_or_invalid_backend_count
    }
}

/// Read-only topology capability used only by the Frontend management surface.
pub trait BackendIslandSnapshotReader: Send + Sync {
    fn backend_island_snapshot(&self) -> BackendIslandSnapshot;
}

/// Starts management observability with the Server-materialized local identity,
/// then installs the topology owner exactly once after application open.
pub struct LateBoundBackendIslandSnapshotReader {
    starting: BackendIslandSnapshot,
    reader: RwLock<Option<Arc<dyn BackendIslandSnapshotReader>>>,
}

impl LateBoundBackendIslandSnapshotReader {
    pub fn new(native_compatibility_id: NativeCompatibilityId) -> Self {
        Self {
            starting: BackendIslandSnapshot::starting(native_compatibility_id),
            reader: RwLock::new(None),
        }
    }

    pub fn install(&self, reader: Arc<dyn BackendIslandSnapshotReader>) -> Result<(), String> {
        let mut slot = self
            .reader
            .write()
            .expect("frontend island reader lock poisoned");
        if slot.is_some() {
            return Err("frontend island snapshot reader is already installed".to_string());
        }
        *slot = Some(reader);
        Ok(())
    }
}

impl BackendIslandSnapshotReader for LateBoundBackendIslandSnapshotReader {
    fn backend_island_snapshot(&self) -> BackendIslandSnapshot {
        self.reader
            .read()
            .expect("frontend island reader lock poisoned")
            .as_ref()
            .map_or(self.starting, |reader| reader.backend_island_snapshot())
    }
}

impl ClusterBackendOpenConfig {
    pub fn new(
        role: ClusterRole,
        native_compatibility_id: NativeCompatibilityId,
        heartbeat_interval: Duration,
        heartbeat_timeout_retries: u32,
        announce_lease_ttl: Duration,
    ) -> Result<Self, String> {
        if heartbeat_interval.is_zero()
            || heartbeat_timeout_retries == 0
            || announce_lease_ttl.is_zero()
        {
            return Err(
                "cluster backend heartbeat and announce lease configuration must be non-zero"
                    .to_string(),
            );
        }
        Ok(Self {
            role,
            native_compatibility_id,
            heartbeat_interval,
            heartbeat_timeout_retries,
            announce_lease_ttl,
        })
    }
    pub const fn role(&self) -> ClusterRole {
        self.role
    }
    pub const fn native_compatibility_id(&self) -> NativeCompatibilityId {
        self.native_compatibility_id
    }
    pub const fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }
    pub const fn heartbeat_timeout_retries(&self) -> u32 {
        self.heartbeat_timeout_retries
    }
    pub const fn announce_lease_ttl(&self) -> Duration {
        self.announce_lease_ttl
    }
}

type HeartbeatProbe =
    dyn Fn(RuntimeEndpoint, BackendProcessId) -> HeartbeatOutcome + Send + Sync + 'static;

#[derive(Clone, Debug, Eq, PartialEq)]
enum Compatibility {
    Unknown,
    Compatible,
    /// The backend speaks a valid native protocol, but belongs to another
    /// exact compatibility island. This is an expected rolling-upgrade state,
    /// not a malformed deployment.
    OtherIsland {
        local: NativeCompatibilityId,
        observed: NativeCompatibilityId,
    },
    Incompatible(String),
}
impl Compatibility {
    fn is_compatible(&self) -> bool {
        matches!(self, Self::Compatible)
    }
    fn detail(&self) -> String {
        match self {
            Self::Unknown => "awaiting exact heartbeat".to_string(),
            Self::Compatible => String::new(),
            Self::OtherIsland { local, observed } => {
                // Keep the full identities out of labels/metrics, but make
                // the read-only topology surface useful for rollout triage.
                // The IDs are fixed-width and validated by the protocol
                // boundary before a descriptor reaches this owner.
                format!("other compatibility island (local {local}, observed {observed})")
            }
            Self::Incompatible(detail) => detail.clone(),
        }
    }
}

#[derive(Clone, Debug)]
struct BackendFacts {
    descriptor: BackendProcessDescriptor,
    announce_lease_valid: bool,
    announce_lease_expires_at: std::time::Instant,
    last_announce_ms: i64,
    exact_identity_verified: bool,
    reported_state: BackendReportedState,
    compatibility: Compatibility,
    endpoint_owned: bool,
    /// An exact replacement has transferred this endpoint to another process.
    /// A stale old process may not reclaim it by announcing again.
    superseded: bool,
    num_cores: u32,
    admission_epoch_capability: Option<AdmissionEpochCapability>,
    last_heartbeat_ms: i64,
    missed_heartbeats: u32,
    scheduled_fragments: u64,
    last_err: Option<String>,
}

impl BackendFacts {
    fn eligible(&self) -> bool {
        self.announce_lease_valid
            && self.exact_identity_verified
            && self.reported_state == BackendReportedState::Running
            && self.compatibility.is_compatible()
            && self.endpoint_owned
            && self.admission_epoch_capability.is_some()
    }
}

struct TopologyState {
    timeout_retries: u32,
    revision: u64,
    terminal_error: Option<String>,
    // Primary index: a process is never inferred from an address.
    processes: BTreeMap<BackendProcessId, BackendFacts>,
    // Verified endpoint owner only. This is part of the eligibility predicate.
    endpoint_owners: BTreeMap<RuntimeEndpoint, BackendProcessId>,
    // Announce creates a pending replacement; exact pull transfers ownership.
    pending_endpoint_owners: BTreeMap<RuntimeEndpoint, BackendProcessId>,
}

#[derive(Clone, Copy)]
struct HeartbeatSignal {
    generation: u64,
    stopping: bool,
}

pub(crate) struct ClusterBackendService {
    state: Mutex<TopologyState>,
    native_compatibility_id: NativeCompatibilityId,
    heartbeat_interval: Duration,
    announce_lease_ttl: Duration,
    heartbeat_probe: Arc<HeartbeatProbe>,
    channel_invalidator: Arc<dyn Fn(&NativeEndpoint) + Send + Sync>,
    heartbeat_thread: Mutex<Option<JoinHandle<()>>>,
    heartbeat_round: Mutex<()>,
    heartbeat_signal: Mutex<HeartbeatSignal>,
    heartbeat_wake: Condvar,
    topology_wake: Condvar,
    process_epoch: watch::Sender<u64>,
    #[cfg(test)]
    _test_runtime_owner: Option<Arc<tokio::runtime::Runtime>>,
}

impl ClusterBackendService {
    pub(crate) fn announce_lease_ttl_ms(&self) -> u64 {
        self.announce_lease_ttl
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }

    pub(crate) async fn open(
        config: ClusterBackendOpenConfig,
        runtime: Handle,
        data_runtime: FrontendDataRuntime,
    ) -> Result<Arc<Self>, String> {
        if config.role() == ClusterRole::Be {
            return Err("role=be must not open ClusterBackendService".to_string());
        }
        let heartbeat_runtime = data_runtime.clone();
        let heartbeat_timeout = config.heartbeat_interval();
        let service = Arc::new(Self::new(
            &config,
            move |endpoint, process_id| {
                native_heartbeat(&heartbeat_runtime, process_id, endpoint, heartbeat_timeout)
            },
            move |endpoint| data_runtime.invalidate_channel(endpoint),
        ));
        let _ = runtime;
        // Only a BE can create its immutable ProcessId descriptor through
        // AnnounceBackend.
        service.publish_snapshot();
        Ok(service)
    }

    fn new<F, I>(config: &ClusterBackendOpenConfig, probe: F, invalidate_channel: I) -> Self
    where
        F: Fn(RuntimeEndpoint, BackendProcessId) -> HeartbeatOutcome + Send + Sync + 'static,
        I: Fn(&NativeEndpoint) + Send + Sync + 'static,
    {
        let (process_epoch, _) = watch::channel(0);
        Self {
            state: Mutex::new(TopologyState {
                timeout_retries: config.heartbeat_timeout_retries(),
                revision: 0,
                terminal_error: None,
                processes: BTreeMap::new(),
                endpoint_owners: BTreeMap::new(),
                pending_endpoint_owners: BTreeMap::new(),
            }),
            native_compatibility_id: config.native_compatibility_id(),
            heartbeat_interval: config.heartbeat_interval(),
            announce_lease_ttl: config.announce_lease_ttl(),
            heartbeat_probe: Arc::new(probe),
            channel_invalidator: Arc::new(invalidate_channel),
            heartbeat_thread: Mutex::new(None),
            heartbeat_round: Mutex::new(()),
            heartbeat_signal: Mutex::new(HeartbeatSignal {
                generation: 0,
                stopping: false,
            }),
            heartbeat_wake: Condvar::new(),
            topology_wake: Condvar::new(),
            process_epoch,
            #[cfg(test)]
            _test_runtime_owner: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_transient_for_test(timeout_retries: u32) -> Self {
        let config = ClusterBackendOpenConfig::new(
            ClusterRole::Fe,
            novarocks_types::NativeCompatibilityId::new([0x71; 32]),
            Duration::from_millis(1),
            timeout_retries.max(1),
            Duration::from_secs(1),
        )
        .unwrap();
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap(),
        );
        let handle = runtime.handle().clone();
        let data_runtime = FrontendDataRuntime::new(handle);
        let heartbeat_timeout = config.heartbeat_interval();
        let mut service = Self::new(
            &config,
            move |endpoint, process_id| {
                native_heartbeat(&data_runtime, process_id, endpoint, heartbeat_timeout)
            },
            |_| {},
        );
        service._test_runtime_owner = Some(runtime);
        service
    }

    #[cfg(test)]
    pub(crate) fn from_captured_targets_for_test(targets: &[LiveBackendTarget]) -> Self {
        let service = Self::new_transient_for_test(1);
        let mut state = service.state.lock().unwrap();
        for target in targets {
            let process_id = target.process_id().expect("captured process id");
            let endpoint = target.endpoint().expect("captured endpoint");
            state.endpoint_owners.insert(endpoint.clone(), process_id);
            state.processes.insert(
                process_id,
                BackendFacts {
                    descriptor: target.descriptor().clone(),
                    announce_lease_valid: true,
                    announce_lease_expires_at: std::time::Instant::now()
                        + service.announce_lease_ttl,
                    last_announce_ms: now_ms(),
                    exact_identity_verified: true,
                    reported_state: BackendReportedState::Running,
                    compatibility: Compatibility::Compatible,
                    endpoint_owned: true,
                    superseded: false,
                    num_cores: 0,
                    admission_epoch_capability: Some(target.admission_epoch_capability()),
                    last_heartbeat_ms: 0,
                    missed_heartbeats: 0,
                    scheduled_fragments: 0,
                    last_err: None,
                },
            );
        }
        drop(state);
        service
    }

    /// Announce establishes a lease and a pending candidate, never eligibility.
    pub(crate) fn record_announce(
        &self,
        descriptor: BackendProcessDescriptor,
        reported_state: BackendReportedState,
    ) -> Result<(), String> {
        let result = self.record_announce_inner(descriptor, reported_state);
        record_backend_announce(if result.is_ok() {
            "accepted"
        } else {
            "rejected"
        });
        result
    }

    fn record_announce_inner(
        &self,
        descriptor: BackendProcessDescriptor,
        reported_state: BackendReportedState,
    ) -> Result<(), String> {
        if reported_state == BackendReportedState::Unspecified {
            return Err("announced backend state must be running or draining".to_string());
        }
        let endpoint = descriptor_runtime_endpoint(&descriptor)?;
        let process_id = descriptor
            .process_id()
            .map_err(|error| format!("invalid announced backend process id: {error}"))?;
        self.refresh_expired_announce_leases(std::time::Instant::now());
        let mut state = self
            .state
            .lock()
            .map_err(|_| "lock frontend topology failed".to_string())?;
        let before = revision_members(&state);
        let old = state.processes.get(&process_id).cloned();
        if old.as_ref().is_some_and(|facts| facts.superseded) {
            return Err(format!(
                "announced backend process {process_id} was already superseded at its endpoint"
            ));
        }
        if old
            .as_ref()
            .is_some_and(|facts| facts.descriptor.as_proto() != descriptor.as_proto())
        {
            return Err(format!(
                "announced backend process {process_id} changed its immutable descriptor"
            ));
        }
        let same_descriptor = old
            .as_ref()
            .is_some_and(|facts| facts.descriptor.as_proto() == descriptor.as_proto());
        let endpoint_owned = state.endpoint_owners.get(&endpoint) == Some(&process_id);
        let facts = state
            .processes
            .entry(process_id)
            .or_insert_with(|| BackendFacts {
                descriptor: descriptor.clone(),
                announce_lease_valid: true,
                announce_lease_expires_at: std::time::Instant::now() + self.announce_lease_ttl,
                last_announce_ms: now_ms(),
                exact_identity_verified: false,
                reported_state,
                compatibility: Compatibility::Unknown,
                endpoint_owned,
                superseded: false,
                num_cores: 0,
                admission_epoch_capability: None,
                last_heartbeat_ms: 0,
                missed_heartbeats: 0,
                scheduled_fragments: 0,
                last_err: None,
            });
        facts.descriptor = descriptor;
        facts.announce_lease_valid = true;
        facts.announce_lease_expires_at = std::time::Instant::now() + self.announce_lease_ttl;
        facts.last_announce_ms = now_ms();
        facts.reported_state = latched_reported_state(facts.reported_state, reported_state);
        facts.endpoint_owned = endpoint_owned;
        if !same_descriptor {
            facts.exact_identity_verified = false;
            facts.compatibility = Compatibility::Unknown;
        }
        if !endpoint_owned {
            state.pending_endpoint_owners.insert(endpoint, process_id);
        } else {
            state.pending_endpoint_owners.remove(&endpoint);
        }
        let changed = advance_if_membership_changed(&mut state, before)?;
        drop(state);
        if changed {
            self.publish_snapshot();
        }
        self.wake_heartbeat_manager();
        Ok(())
    }

    pub(crate) fn start_heartbeat_manager(self: &Arc<Self>) -> Result<(), String> {
        let mut thread = self
            .heartbeat_thread
            .lock()
            .map_err(|_| "lock frontend topology heartbeat thread failed".to_string())?;
        if thread.is_some() {
            return Ok(());
        }
        self.heartbeat_signal
            .lock()
            .map_err(|_| "lock frontend topology heartbeat signal failed".to_string())?
            .stopping = false;
        let service = Arc::clone(self);
        let interval = self.heartbeat_interval;
        *thread = Some(
            std::thread::Builder::new()
                .name("frontend-heartbeat-manager".to_string())
                .spawn(move || service.run_heartbeat_manager(interval))
                .map_err(|error| format!("spawn frontend heartbeat manager failed: {error}"))?,
        );
        Ok(())
    }

    pub(crate) async fn stop_heartbeat_manager_until(
        &self,
        deadline: std::time::Instant,
    ) -> Result<(), String> {
        self.request_heartbeat_stop_for_process_exit()?;
        loop {
            let finished = self
                .heartbeat_thread
                .lock()
                .map_err(|_| "lock frontend topology heartbeat thread failed".to_string())?
                .as_ref()
                .is_none_or(std::thread::JoinHandle::is_finished);
            if finished {
                break;
            }
            if std::time::Instant::now() >= deadline {
                return Err(
                    "frontend heartbeat manager did not stop before the shared shutdown deadline"
                        .to_string(),
                );
            }
            tokio::time::sleep(
                Duration::from_millis(10)
                    .min(deadline.saturating_duration_since(std::time::Instant::now())),
            )
            .await;
        }
        let join = self
            .heartbeat_thread
            .lock()
            .map_err(|_| "lock frontend topology heartbeat thread failed".to_string())?
            .take();
        if let Some(join) = join {
            join.join()
                .map_err(|payload| format!("frontend heartbeat manager panicked: {payload:?}"))?;
        }
        Ok(())
    }

    pub(crate) fn request_heartbeat_stop_for_process_exit(&self) -> Result<(), String> {
        {
            let mut signal = self
                .heartbeat_signal
                .lock()
                .map_err(|_| "lock frontend topology heartbeat signal failed".to_string())?;
            signal.stopping = true;
            signal.generation = signal.generation.wrapping_add(1);
        }
        self.heartbeat_wake.notify_all();
        Ok(())
    }

    fn run_heartbeat_manager(&self, interval: Duration) {
        let mut observed_generation = 0;
        loop {
            if self.heartbeat_is_stopping() {
                return;
            }
            self.refresh_expired_announce_leases(std::time::Instant::now());
            {
                let _round = self
                    .heartbeat_round
                    .lock()
                    .unwrap_or_else(|p| p.into_inner());
                for (process_id, endpoint) in self.heartbeat_rows() {
                    match (self.heartbeat_probe)(endpoint, process_id) {
                        HeartbeatOutcome::Ok {
                            descriptor,
                            reported_state,
                            num_cores,
                            admission_epoch_capability,
                            now_ms,
                        } => self.record_heartbeat_success(
                            process_id,
                            descriptor,
                            reported_state,
                            num_cores,
                            admission_epoch_capability,
                            now_ms,
                        ),
                        HeartbeatOutcome::Failed { err } => {
                            self.record_heartbeat_failure_with_error(process_id, err);
                        }
                    }
                }
            }
            let signal = self
                .heartbeat_signal
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if signal.stopping {
                return;
            }
            let signal = if signal.generation == observed_generation {
                self.heartbeat_wake
                    .wait_timeout_while(signal, interval, |s| {
                        !s.stopping && s.generation == observed_generation
                    })
                    .unwrap_or_else(|p| p.into_inner())
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

    fn heartbeat_rows(&self) -> Vec<(BackendProcessId, RuntimeEndpoint)> {
        let state = self.state.lock().unwrap();
        state
            .processes
            .iter()
            .filter_map(|(id, facts)| {
                facts
                    .announce_lease_valid
                    .then(|| {
                        descriptor_runtime_endpoint(&facts.descriptor)
                            .ok()
                            .map(|endpoint| (*id, endpoint))
                    })
                    .flatten()
            })
            .collect()
    }
    fn heartbeat_is_stopping(&self) -> bool {
        self.heartbeat_signal
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .stopping
    }
    fn wake_heartbeat_manager(&self) {
        let mut signal = self
            .heartbeat_signal
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        signal.generation = signal.generation.wrapping_add(1);
        drop(signal);
        self.heartbeat_wake.notify_all();
    }

    pub(crate) fn record_heartbeat_success(
        &self,
        process_id: BackendProcessId,
        descriptor: BackendProcessDescriptor,
        reported_state: BackendReportedState,
        num_cores: u32,
        admission_epoch_capability: AdmissionEpochCapability,
        now_ms: i64,
    ) {
        self.refresh_expired_announce_leases(std::time::Instant::now());
        let mut state = self.state.lock().unwrap();
        let before = revision_members(&state);
        let Some(announced) = state.processes.get(&process_id).cloned() else {
            record_backend_heartbeat("unknown_process");
            return;
        };
        let exact = descriptor.as_proto() == announced.descriptor.as_proto();
        let compatibility = if !exact {
            Compatibility::Incompatible("heartbeat descriptor does not match announce".to_string())
        } else {
            match descriptor.native_compatibility_id() {
                Ok(observed) if observed == self.native_compatibility_id => {
                    Compatibility::Compatible
                }
                Ok(observed) => Compatibility::OtherIsland {
                    local: self.native_compatibility_id,
                    observed,
                },
                Err(error) => Compatibility::Incompatible(format!(
                    "backend compatibility identity is invalid: {error}"
                )),
            }
        };
        let Ok(endpoint) = descriptor_runtime_endpoint(&announced.descriptor) else {
            return;
        };
        let transfer = exact
            && compatibility.is_compatible()
            && reported_state == BackendReportedState::Running
            && state.pending_endpoint_owners.get(&endpoint) == Some(&process_id);
        let old_owner = transfer
            .then(|| state.endpoint_owners.get(&endpoint).copied())
            .flatten();
        let replaced = transfer && old_owner.is_some_and(|old| old != process_id);
        if transfer {
            state.pending_endpoint_owners.remove(&endpoint);
            state.endpoint_owners.insert(endpoint.clone(), process_id);
            if let Some(old) = old_owner
                .filter(|old| *old != process_id)
                .and_then(|old| state.processes.get_mut(&old))
            {
                old.endpoint_owned = false;
                old.superseded = true;
            }
        }
        let endpoint_owned = state.endpoint_owners.get(&endpoint) == Some(&process_id);
        if let Some(facts) = state.processes.get_mut(&process_id) {
            facts.exact_identity_verified = exact;
            facts.reported_state = latched_reported_state(facts.reported_state, reported_state);
            facts.compatibility = compatibility;
            facts.endpoint_owned = endpoint_owned;
            facts.num_cores = num_cores;
            facts.admission_epoch_capability = Some(admission_epoch_capability);
            facts.last_heartbeat_ms = now_ms;
            facts.missed_heartbeats = 0;
            facts.last_err = None;
        }
        let changed = advance_if_membership_changed(&mut state, before).unwrap_or(false);
        drop(state);
        if replaced {
            (self.channel_invalidator)(endpoint.native_endpoint());
        }
        if changed {
            self.publish_snapshot();
        }
        record_backend_heartbeat(if exact {
            "verified"
        } else {
            "identity_mismatch"
        });
    }

    #[cfg(test)]
    pub(crate) fn record_heartbeat_failure(&self, process_id: BackendProcessId) -> bool {
        self.record_heartbeat_failure_with_error(process_id, "heartbeat failed")
    }
    fn record_heartbeat_failure_with_error(
        &self,
        process_id: BackendProcessId,
        error: impl Into<String>,
    ) -> bool {
        record_backend_heartbeat("failed");
        self.refresh_expired_announce_leases(std::time::Instant::now());
        let mut state = self.state.lock().unwrap();
        let before = revision_members(&state);
        let timeout = state.timeout_retries;
        let Some(facts) = state.processes.get_mut(&process_id) else {
            return false;
        };
        facts.missed_heartbeats = facts.missed_heartbeats.saturating_add(1);
        facts.last_err = Some(error.into());
        if facts.missed_heartbeats >= timeout {
            facts.exact_identity_verified = false;
        }
        let changed = advance_if_membership_changed(&mut state, before).unwrap_or(false);
        drop(state);
        // Loss affects future admission but is not a query failure event.
        if changed {
            self.publish_snapshot();
        }
        changed
    }

    fn publish_snapshot(&self) {
        let (metrics, revision) = {
            let state = self.state.lock().unwrap();
            (metrics_snapshot(&state), state.revision)
        };
        publish_backend_topology_metrics(metrics);
        self.process_epoch.send_replace(revision);
        self.topology_wake.notify_all();
    }

    fn refresh_expired_announce_leases(&self, now: std::time::Instant) -> bool {
        let mut state = self.state.lock().unwrap();
        let before = revision_members(&state);
        for facts in state.processes.values_mut() {
            if facts.announce_lease_valid && facts.announce_lease_expires_at <= now {
                facts.announce_lease_valid = false;
            }
        }
        let changed = advance_if_membership_changed(&mut state, before).unwrap_or(false);
        drop(state);
        if changed {
            self.publish_snapshot();
        }
        changed
    }

    fn snapshot_inner(&self) -> Result<BackendTopologySnapshot, BackendTopologyError> {
        let state = self
            .state
            .lock()
            .map_err(|_| BackendTopologyError::Unavailable {
                message: "lock frontend topology failed".to_string(),
            })?;
        if let Some(message) = &state.terminal_error {
            return Err(BackendTopologyError::Unavailable {
                message: message.clone(),
            });
        }
        BackendTopologySnapshot::try_new(state.revision, live_targets(&state))
    }

    fn island_snapshot_inner(&self) -> BackendIslandSnapshot {
        let state = self.state.lock().expect("frontend topology lock poisoned");
        let mut snapshot = BackendIslandSnapshot {
            native_compatibility_id: self.native_compatibility_id,
            topology_revision: state.revision,
            compatible_eligible_backend_count: 0,
            other_island_backend_count: 0,
            unknown_or_invalid_backend_count: 0,
        };
        for facts in state.processes.values() {
            match facts.compatibility {
                Compatibility::Compatible => {
                    snapshot.compatible_eligible_backend_count += usize::from(facts.eligible());
                }
                Compatibility::OtherIsland { .. } => snapshot.other_island_backend_count += 1,
                Compatibility::Unknown | Compatibility::Incompatible(_) => {
                    snapshot.unknown_or_invalid_backend_count += 1;
                }
            }
        }
        snapshot
    }
}

impl BackendIslandSnapshotReader for ClusterBackendService {
    fn backend_island_snapshot(&self) -> BackendIslandSnapshot {
        self.refresh_expired_announce_leases(std::time::Instant::now());
        self.island_snapshot_inner()
    }
}

impl BackendTopologyPort for ClusterBackendService {
    fn snapshot(&self) -> Result<BackendTopologySnapshot, BackendTopologyError> {
        self.refresh_expired_announce_leases(std::time::Instant::now());
        self.snapshot_inner()
    }

    fn subscribe_changes(&self) -> watch::Receiver<u64> {
        self.process_epoch.subscribe()
    }
    fn validate_snapshot(
        &self,
        expected: &BackendTopologySnapshot,
    ) -> Result<(), BackendTopologyValidationError> {
        self.refresh_expired_announce_leases(std::time::Instant::now());
        let current = self
            .snapshot_inner()
            .map_err(BackendTopologyValidationError::Unavailable)?;
        if current == *expected {
            return Ok(());
        }
        // A revision advance is not sufficient evidence to retry an attempt.
        // Preserve the exact captured process evidence when a planned target
        // disappeared or was replaced, so the pre-ready coordinator can make
        // its one bounded whole-round retry decision without parsing a
        // transport error.
        for expected_target in expected.targets() {
            let expected_process_id = expected_target.process_id().map_err(|_| {
                BackendTopologyValidationError::ContentChangedWithoutRevision {
                    revision: expected.revision(),
                }
            })?;
            let Some(current_target) = current.target(expected_target.backend_idx()) else {
                return Err(BackendTopologyValidationError::TargetMissing {
                    backend_idx: expected_target.backend_idx(),
                    captured_generation: expected_process_id,
                    captured_revision: expected.revision(),
                    current_revision: current.revision(),
                });
            };
            let current_process_id = current_target.process_id().map_err(|_| {
                BackendTopologyValidationError::ContentChangedWithoutRevision {
                    revision: current.revision(),
                }
            })?;
            if current_process_id != expected_process_id {
                return Err(BackendTopologyValidationError::GenerationChanged {
                    backend_idx: expected_target.backend_idx(),
                    captured_generation: expected_process_id,
                    current_generation: current_process_id,
                    captured_revision: expected.revision(),
                    current_revision: current.revision(),
                });
            }
        }
        if current.revision() != expected.revision() {
            return Err(BackendTopologyValidationError::RevisionChanged {
                captured_revision: expected.revision(),
                current_revision: current.revision(),
            });
        }
        Err(
            BackendTopologyValidationError::ContentChangedWithoutRevision {
                revision: current.revision(),
            },
        )
    }

    fn wait_for_eligible_after(
        &self,
        revision: u64,
        deadline: std::time::Instant,
    ) -> Result<BackendTopologySnapshot, BackendTopologyError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| BackendTopologyError::Unavailable {
                message: "lock frontend topology failed".to_string(),
            })?;
        loop {
            if let Some(message) = &state.terminal_error {
                return Err(BackendTopologyError::Unavailable {
                    message: message.clone(),
                });
            }
            let snapshot = BackendTopologySnapshot::try_new(state.revision, live_targets(&state))?;
            if snapshot.revision() > revision && !snapshot.targets().is_empty() {
                return Ok(snapshot);
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                return Err(BackendTopologyError::Unavailable {
                    message: format!(
                        "timed out waiting for an eligible backend topology revision after {revision}"
                    ),
                });
            }
            let (next, _) = self
                .topology_wake
                .wait_timeout(state, deadline.saturating_duration_since(now))
                .expect("frontend topology wait lock");
            state = next;
        }
    }
    fn record_successful_stage(&self, backend_idx: usize, fragment_count: usize) {
        let mut state = self.state.lock().unwrap();
        let process_id = live_targets(&state)
            .get(backend_idx)
            .and_then(|target| target.process_id().ok());
        if let Some(facts) = process_id.and_then(|id| state.processes.get_mut(&id)) {
            facts.scheduled_fragments = facts
                .scheduled_fragments
                .saturating_add(fragment_count as u64);
        }
        crate::common::backend_topology::record_successful_stage(backend_idx, fragment_count);
    }
    fn show_backends(&self) -> Result<QueryResult, String> {
        self.refresh_expired_announce_leases(std::time::Instant::now());
        let state = self
            .state
            .lock()
            .map_err(|_| "lock frontend topology failed".to_string())?;
        let names = [
            "ProcessId",
            "Endpoint",
            "LeaseValid",
            "IdentityVerified",
            "ReportedState",
            "Compatible",
            "EndpointOwned",
            "Eligible",
            "ScheduledFragments",
            "LastAnnounceAt",
            "LastHeartbeatAt",
            "BuildIdentity",
            "NativeCompatibilityId",
            "DiagnosticStatus",
            "StatusDetail",
        ];
        let mut columns = vec![Vec::new(); names.len()];
        for (process_id, facts) in &state.processes {
            let endpoint = facts
                .descriptor
                .endpoint()
                .map_err(|error| format!("invalid registered endpoint: {error}"))?;
            columns[0].push(process_id.to_string());
            columns[1].push(format!("{}:{}", endpoint.host(), endpoint.port()));
            columns[2].push(facts.announce_lease_valid.to_string());
            columns[3].push(facts.exact_identity_verified.to_string());
            columns[4].push(format!("{:?}", facts.reported_state));
            columns[5].push(facts.compatibility.is_compatible().to_string());
            columns[6].push(facts.endpoint_owned.to_string());
            columns[7].push(facts.eligible().to_string());
            columns[8].push(facts.scheduled_fragments.to_string());
            columns[9].push(facts.last_announce_ms.to_string());
            columns[10].push(facts.last_heartbeat_ms.to_string());
            columns[11].push(facts.descriptor.build_identity().to_string());
            columns[12].push(
                facts
                    .descriptor
                    .native_compatibility_id()
                    .map_err(|error| format!("invalid registered compatibility identity: {error}"))?
                    .to_string(),
            );
            columns[13].push(diagnostic_status(facts));
            columns[14].push(
                facts
                    .last_err
                    .clone()
                    .unwrap_or_else(|| facts.compatibility.detail().to_string()),
            );
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

impl BackendProcessObservationPort for ClusterBackendService {
    fn subscribe_process_changes(&self) -> watch::Receiver<u64> {
        self.process_epoch.subscribe()
    }

    fn observe_process_at_endpoint(
        &self,
        expected_process: BackendProcessId,
        expected_endpoint: &RuntimeEndpoint,
    ) -> Result<BackendProcessObservation, BackendTopologyError> {
        self.refresh_expired_announce_leases(std::time::Instant::now());
        let state = self
            .state
            .lock()
            .map_err(|_| BackendTopologyError::Unavailable {
                message: "lock frontend topology failed".to_string(),
            })?;
        if let Some(message) = &state.terminal_error {
            return Err(BackendTopologyError::Unavailable {
                message: message.clone(),
            });
        }
        let Some(current_process) = state.endpoint_owners.get(expected_endpoint).copied() else {
            return Ok(BackendProcessObservation::Unobservable);
        };
        if current_process != expected_process {
            return Ok(BackendProcessObservation::Replaced { current_process });
        }
        Ok(
            if state
                .processes
                .get(&expected_process)
                .is_some_and(BackendFacts::eligible)
            {
                BackendProcessObservation::Current
            } else {
                BackendProcessObservation::Unobservable
            },
        )
    }
}

fn latched_reported_state(
    current: BackendReportedState,
    observed: BackendReportedState,
) -> BackendReportedState {
    if current == BackendReportedState::Draining || observed == BackendReportedState::Draining {
        BackendReportedState::Draining
    } else {
        observed
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

fn diagnostic_status(facts: &BackendFacts) -> String {
    let mut states = Vec::new();
    if !facts.announce_lease_valid {
        states.push("Stale");
    }
    if !facts.exact_identity_verified {
        states.push("Lost");
    }
    if facts.reported_state == BackendReportedState::Draining {
        states.push("Draining");
    }
    match facts.compatibility {
        Compatibility::OtherIsland { .. } => states.push("OtherIsland"),
        Compatibility::Unknown => states.push("CompatibilityUnknown"),
        Compatibility::Incompatible(_) => states.push("Incompatible"),
        Compatibility::Compatible => {}
    }
    if facts.superseded {
        states.push("Replaced");
    }
    if states.is_empty() {
        "Live".to_string()
    } else {
        states.join("|")
    }
}

fn descriptor_runtime_endpoint(
    descriptor: &BackendProcessDescriptor,
) -> Result<RuntimeEndpoint, String> {
    let endpoint = descriptor
        .endpoint()
        .map_err(|error| format!("announced backend endpoint is invalid: {error}"))?;
    RuntimeEndpoint::new(endpoint.host(), i32::from(endpoint.port()))
        .map_err(|error| format!("announced backend endpoint is invalid: {error}"))
}
fn live_targets(state: &TopologyState) -> Vec<LiveBackendTarget> {
    state
        .endpoint_owners
        .values()
        .filter_map(|process_id| state.processes.get(process_id))
        .filter(|facts| facts.eligible())
        .enumerate()
        .map(|(membership_ordinal, facts)| {
            LiveBackendTarget::new(
                membership_ordinal,
                facts.descriptor.clone(),
                facts
                    .admission_epoch_capability
                    .expect("eligible backend has an admission epoch capability"),
            )
        })
        .collect()
}
fn metrics_snapshot(state: &TopologyState) -> BackendTopologyMetricsSnapshot {
    let mut metrics = BackendTopologyMetricsSnapshot {
        entries: state.processes.len(),
        revision: state.revision,
        ..BackendTopologyMetricsSnapshot::default()
    };
    for facts in state.processes.values() {
        metrics.announce_lease_valid += usize::from(facts.announce_lease_valid);
        metrics.identity_verified += usize::from(facts.exact_identity_verified);
        match facts.reported_state {
            BackendReportedState::Running => metrics.reported_running += 1,
            BackendReportedState::Draining => metrics.reported_draining += 1,
            BackendReportedState::Unspecified => {}
        }
        match facts.compatibility {
            Compatibility::Compatible => metrics.compatibility_compatible += 1,
            Compatibility::OtherIsland { .. } => metrics.compatibility_other_island += 1,
            Compatibility::Unknown | Compatibility::Incompatible(_) => {
                metrics.compatibility_unknown_or_invalid += 1
            }
        }
        metrics.endpoint_owned += usize::from(facts.endpoint_owned);
        metrics.endpoint_unowned += usize::from(!facts.endpoint_owned);
        if facts.eligible() {
            metrics.eligible += 1;
        }
    }
    metrics
}
/// A topology revision must make a captured execution snapshot stale whenever
/// either its schedulable membership changes or an exact-heartbeat backend
/// moves into/out of a different compatibility island.  The latter is not a
/// target today, but publishing it makes a rolling-upgrade observation visible
/// to the same immutable snapshot boundary without treating OtherIsland as an
/// invalid deployment.
fn advance_if_membership_changed(
    state: &mut TopologyState,
    before: BTreeSet<(
        BackendProcessId,
        RuntimeEndpoint,
        u8,
        Option<AdmissionEpochCapability>,
    )>,
) -> Result<bool, String> {
    if before == revision_members(state) {
        return Ok(false);
    }
    if let Some(message) = &state.terminal_error {
        return Err(message.clone());
    }
    state.revision = state.revision.checked_add(1).ok_or_else(|| {
        let message = "frontend topology revision space is exhausted".to_string();
        state.terminal_error = Some(message.clone());
        message
    })?;
    Ok(true)
}

fn revision_members(
    state: &TopologyState,
) -> BTreeSet<(
    BackendProcessId,
    RuntimeEndpoint,
    u8,
    Option<AdmissionEpochCapability>,
)> {
    state
        .processes
        .iter()
        .filter_map(|(id, facts)| {
            if !facts.announce_lease_valid || !facts.exact_identity_verified {
                return None;
            }
            let category = match facts.compatibility {
                Compatibility::Compatible if facts.eligible() => 1,
                Compatibility::OtherIsland { .. } => 2,
                _ => return None,
            };
            descriptor_runtime_endpoint(&facts.descriptor)
                .ok()
                .map(|endpoint| (*id, endpoint, category, facts.admission_epoch_capability))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{BackendIslandSnapshotReader, ClusterBackendService};
    use crate::common::backend_topology::{
        BackendProcessObservation, BackendProcessObservationPort, BackendTopologyPort,
    };
    use novarocks_execution::task_execution::AdmissionEpochCapability;
    use novarocks_proto_codec::lifecycle::QueryControlEndpoint;
    use novarocks_proto_codec::membership::{BackendProcessDescriptor, BackendReportedState};
    use novarocks_types::BackendProcessId;
    use novarocks_version::native_build_identity;
    use std::net::SocketAddr;
    use std::sync::Arc;
    fn descriptor(endpoint: SocketAddr) -> BackendProcessDescriptor {
        descriptor_with(
            endpoint,
            native_build_identity(),
            novarocks_types::NativeCompatibilityId::new([0x71; 32]),
        )
    }

    fn descriptor_with(
        endpoint: SocketAddr,
        build_identity: impl Into<String>,
        native_compatibility_id: novarocks_types::NativeCompatibilityId,
    ) -> BackendProcessDescriptor {
        BackendProcessDescriptor::new(
            BackendProcessId::new_v7(),
            QueryControlEndpoint::new(endpoint.ip().to_string(), endpoint.port()).unwrap(),
            "test",
            build_identity,
            native_compatibility_id,
        )
        .unwrap()
    }
    fn verify(service: &ClusterBackendService, descriptor: &BackendProcessDescriptor) {
        service.record_heartbeat_success(
            descriptor.process_id().unwrap(),
            descriptor.clone(),
            BackendReportedState::Running,
            2,
            AdmissionEpochCapability::try_from_bytes([0x61; 16]).expect("nonzero epoch"),
            1,
        );
    }

    #[tokio::test]
    async fn shared_deadline_retains_the_same_heartbeat_join_for_retry() {
        let service = ClusterBackendService::new_transient_for_test(1);
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let join = std::thread::spawn(move || {
            release_rx.recv().expect("release heartbeat test worker");
        });
        *service.heartbeat_thread.lock().unwrap() = Some(join);

        let error = service
            .stop_heartbeat_manager_until(
                std::time::Instant::now() + std::time::Duration::from_millis(20),
            )
            .await
            .expect_err("stuck heartbeat worker must honor the shared deadline");
        assert!(error.contains("shared shutdown deadline"));
        assert!(service.heartbeat_thread.lock().unwrap().is_some());

        release_tx.send(()).unwrap();
        service
            .stop_heartbeat_manager_until(
                std::time::Instant::now() + std::time::Duration::from_secs(1),
            )
            .await
            .expect("retry joins the retained heartbeat worker");
        assert!(service.heartbeat_thread.lock().unwrap().is_none());
        tokio::task::spawn_blocking(move || drop(service))
            .await
            .unwrap();
    }

    #[test]
    fn announcement_is_not_eligible_until_exact_pull() {
        let service = ClusterBackendService::new_transient_for_test(1);
        let descriptor = descriptor("127.0.0.1:9070".parse().unwrap());
        service
            .record_announce(descriptor.clone(), BackendReportedState::Running)
            .unwrap();
        assert!(service.snapshot().unwrap().targets().is_empty());
        verify(&service, &descriptor);
        assert_eq!(service.snapshot().unwrap().targets().len(), 1);
    }

    #[test]
    fn admission_epoch_rotation_advances_the_frozen_topology_revision() {
        let service = ClusterBackendService::new_transient_for_test(1);
        let descriptor = descriptor("127.0.0.1:9079".parse().unwrap());
        service
            .record_announce(descriptor.clone(), BackendReportedState::Running)
            .unwrap();
        verify(&service, &descriptor);
        let first = service.snapshot().expect("first eligible snapshot");
        let first_revision = first.revision();
        let first_epoch = first.targets()[0].admission_epoch_capability();
        let next_epoch =
            AdmissionEpochCapability::try_from_bytes([0x62; 16]).expect("nonzero next epoch");

        service.record_heartbeat_success(
            descriptor.process_id().unwrap(),
            descriptor,
            BackendReportedState::Running,
            2,
            next_epoch,
            2,
        );

        let second = service.snapshot().expect("rotated eligible snapshot");
        assert!(second.revision() > first_revision);
        assert_ne!(first_epoch, next_epoch);
        assert_eq!(second.targets()[0].admission_epoch_capability(), next_epoch);
        assert_ne!(first, second);
    }

    #[test]
    fn island_snapshot_counts_exact_eligible_and_other_island_from_one_revision() {
        let service = ClusterBackendService::new_transient_for_test(1);
        let compatible = descriptor("127.0.0.1:9070".parse().unwrap());
        let other = descriptor_with(
            "127.0.0.1:9071".parse().unwrap(),
            native_build_identity(),
            novarocks_types::NativeCompatibilityId::new([0x72; 32]),
        );
        service
            .record_announce(compatible.clone(), BackendReportedState::Running)
            .expect("announce compatible backend");
        service
            .record_announce(other.clone(), BackendReportedState::Running)
            .expect("announce other-island backend");
        verify(&service, &compatible);
        verify(&service, &other);

        let snapshot = service.backend_island_snapshot();
        assert_eq!(
            snapshot.native_compatibility_id(),
            novarocks_types::NativeCompatibilityId::new([0x71; 32])
        );
        assert_eq!(snapshot.compatible_eligible_backend_count(), 1);
        assert_eq!(snapshot.other_island_backend_count(), 1);
        assert_eq!(snapshot.unknown_or_invalid_backend_count(), 0);
        assert_eq!(
            snapshot.topology_revision(),
            service.snapshot().unwrap().revision()
        );
    }

    #[test]
    fn wait_for_eligible_after_requires_a_new_verified_revision() {
        let service = Arc::new(ClusterBackendService::new_transient_for_test(1));
        let revision = service.snapshot().expect("initial snapshot").revision();
        let waiter = {
            let service = Arc::clone(&service);
            std::thread::spawn(move || {
                service.wait_for_eligible_after(
                    revision,
                    std::time::Instant::now() + std::time::Duration::from_secs(1),
                )
            })
        };
        let descriptor = descriptor("127.0.0.1:9070".parse().unwrap());
        service
            .record_announce(descriptor.clone(), BackendReportedState::Running)
            .unwrap();
        verify(&service, &descriptor);

        let snapshot = waiter
            .join()
            .expect("topology waiter must not panic")
            .expect("new verified backend must wake waiter");
        assert!(snapshot.revision() > revision);
        assert_eq!(
            snapshot.targets()[0].process_id().unwrap(),
            descriptor.process_id().unwrap()
        );
    }
    #[test]
    fn replacement_is_pending_until_exact_pull_transfers_eligibility() {
        let service = ClusterBackendService::new_transient_for_test(1);
        let endpoint = "127.0.0.1:9070".parse().unwrap();
        let old = descriptor(endpoint);
        service
            .record_announce(old.clone(), BackendReportedState::Running)
            .unwrap();
        verify(&service, &old);
        let new = descriptor(endpoint);
        service
            .record_announce(new.clone(), BackendReportedState::Running)
            .unwrap();
        assert_eq!(
            service.snapshot().unwrap().targets()[0]
                .process_id()
                .unwrap(),
            old.process_id().unwrap()
        );
        verify(&service, &new);
        assert_eq!(
            service.snapshot().unwrap().targets()[0]
                .process_id()
                .unwrap(),
            new.process_id().unwrap()
        );
    }

    #[test]
    fn exact_process_replacement_closes_an_unobservable_endpoint_owner() {
        let service = ClusterBackendService::new_transient_for_test(1);
        let endpoint = "127.0.0.1:9070".parse().unwrap();
        let old = descriptor(endpoint);
        let old_process = old.process_id().unwrap();
        let runtime_endpoint = super::descriptor_runtime_endpoint(&old).unwrap();
        service
            .record_announce(old.clone(), BackendReportedState::Running)
            .unwrap();
        verify(&service, &old);
        assert_eq!(
            service
                .observe_process_at_endpoint(old_process, &runtime_endpoint)
                .unwrap(),
            BackendProcessObservation::Current
        );

        assert!(service.record_heartbeat_failure(old_process));
        assert_eq!(
            service
                .observe_process_at_endpoint(old_process, &runtime_endpoint)
                .unwrap(),
            BackendProcessObservation::Unobservable
        );

        let replacement = descriptor(endpoint);
        let replacement_process = replacement.process_id().unwrap();
        service
            .record_announce(replacement.clone(), BackendReportedState::Running)
            .unwrap();
        verify(&service, &replacement);
        assert_eq!(
            service
                .observe_process_at_endpoint(old_process, &runtime_endpoint)
                .unwrap(),
            BackendProcessObservation::Replaced {
                current_process: replacement_process,
            }
        );
    }

    #[test]
    fn live_backend_ordinals_follow_verified_endpoints_across_replacement() {
        let service = ClusterBackendService::new_transient_for_test(1);
        let higher_endpoint = descriptor("127.0.0.1:9071".parse().unwrap());
        let lower_endpoint = descriptor("127.0.0.1:9070".parse().unwrap());
        service
            .record_announce(higher_endpoint.clone(), BackendReportedState::Running)
            .expect("announce higher endpoint");
        service
            .record_announce(lower_endpoint.clone(), BackendReportedState::Running)
            .expect("announce lower endpoint");
        verify(&service, &higher_endpoint);
        verify(&service, &lower_endpoint);

        let before = service
            .snapshot()
            .expect("initial endpoint-ordered snapshot");
        assert_eq!(
            before.targets()[0]
                .process_id()
                .expect("lower endpoint process"),
            lower_endpoint
                .process_id()
                .expect("lower endpoint descriptor")
        );
        assert_eq!(
            before.targets()[1]
                .process_id()
                .expect("higher endpoint process"),
            higher_endpoint
                .process_id()
                .expect("higher endpoint descriptor")
        );

        let replacement = descriptor("127.0.0.1:9071".parse().unwrap());
        service
            .record_announce(replacement.clone(), BackendReportedState::Running)
            .expect("announce endpoint replacement");
        verify(&service, &replacement);

        let after = service
            .snapshot()
            .expect("replacement endpoint-ordered snapshot");
        assert_eq!(
            after.targets()[0]
                .process_id()
                .expect("lower endpoint process"),
            lower_endpoint
                .process_id()
                .expect("lower endpoint descriptor")
        );
        assert_eq!(
            after.targets()[1]
                .process_id()
                .expect("replacement process"),
            replacement.process_id().expect("replacement descriptor")
        );
    }

    #[test]
    fn replacement_reports_the_captured_process_generation_not_only_revision_drift() {
        let service = ClusterBackendService::new_transient_for_test(1);
        let endpoint = "127.0.0.1:9070".parse().unwrap();
        let old = descriptor(endpoint);
        service
            .record_announce(old.clone(), BackendReportedState::Running)
            .unwrap();
        verify(&service, &old);
        let captured = service.snapshot().unwrap();

        let replacement = descriptor(endpoint);
        service
            .record_announce(replacement.clone(), BackendReportedState::Running)
            .unwrap();
        verify(&service, &replacement);

        assert!(matches!(
            service.validate_snapshot(&captured),
            Err(crate::common::backend_topology::BackendTopologyValidationError::GenerationChanged {
                backend_idx: 0,
                captured_generation,
                current_generation,
                ..
            }) if captured_generation == old.process_id().unwrap()
                && current_generation == replacement.process_id().unwrap()
        ));
    }
    #[test]
    fn heartbeat_loss_never_sends_unavailable() {
        let service = ClusterBackendService::new_transient_for_test(1);
        let descriptor = descriptor("127.0.0.1:9070".parse().unwrap());
        service
            .record_announce(descriptor.clone(), BackendReportedState::Running)
            .unwrap();
        verify(&service, &descriptor);
        assert!(service.record_heartbeat_failure(descriptor.process_id().unwrap()));
        assert!(service.snapshot().unwrap().targets().is_empty());
    }

    #[test]
    fn announce_lease_expiry_removes_only_new_query_eligibility() {
        let service = ClusterBackendService::new_transient_for_test(1);
        let descriptor = descriptor("127.0.0.1:9070".parse().unwrap());
        service
            .record_announce(descriptor.clone(), BackendReportedState::Running)
            .unwrap();
        verify(&service, &descriptor);
        let revision = service.snapshot().unwrap().revision();

        assert!(service.refresh_expired_announce_leases(
            std::time::Instant::now() + std::time::Duration::from_secs(2)
        ));
        let snapshot = service.snapshot().unwrap();
        assert!(snapshot.targets().is_empty());
        assert_eq!(snapshot.revision(), revision + 1);
    }

    #[test]
    fn draining_is_monotonic_across_later_running_observations() {
        let service = ClusterBackendService::new_transient_for_test(1);
        let descriptor = descriptor("127.0.0.1:9070".parse().unwrap());
        service
            .record_announce(descriptor.clone(), BackendReportedState::Running)
            .unwrap();
        verify(&service, &descriptor);

        service
            .record_announce(descriptor.clone(), BackendReportedState::Draining)
            .unwrap();
        service.record_heartbeat_success(
            descriptor.process_id().unwrap(),
            descriptor,
            BackendReportedState::Running,
            2,
            AdmissionEpochCapability::try_from_bytes([0x61; 16]).expect("nonzero epoch"),
            2,
        );

        assert!(service.snapshot().unwrap().targets().is_empty());
        assert_eq!(
            service
                .state
                .lock()
                .unwrap()
                .processes
                .values()
                .next()
                .unwrap()
                .reported_state,
            BackendReportedState::Draining
        );
    }
    /// One `SHOW BACKENDS` row as the cross-process topology barrier reads it.
    ///
    /// The barrier in `tests/cluster-harness` counts rows that satisfy
    /// `is_eligible_live()`: `DiagnosticStatus == "Live"`, `Eligible`, both
    /// identities present, **and an empty `StatusDetail`**. That last clause is
    /// what makes this projection load-bearing rather than diagnostic, so the
    /// test below reads exactly these six fields and nothing else.
    struct BarrierRow {
        process_id: String,
        diagnostic_status: String,
        eligible: bool,
        build_identity: String,
        compatibility_id: String,
        status_detail: String,
    }

    impl BarrierRow {
        fn is_eligible_live(&self) -> bool {
            self.diagnostic_status == "Live"
                && self.eligible
                && !self.build_identity.is_empty()
                && !self.compatibility_id.is_empty()
                && self.status_detail.is_empty()
        }
    }

    fn barrier_rows(service: &ClusterBackendService) -> Vec<BarrierRow> {
        let result = service.show_backends().expect("SHOW BACKENDS projection");
        let names = result
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let index = |name: &str| {
            names
                .iter()
                .position(|candidate| candidate == name)
                .unwrap_or_else(|| panic!("SHOW BACKENDS has no {name} column"))
        };
        let mut rows = Vec::new();
        for chunk in &result.chunks {
            let column = |name: &str| {
                chunk
                    .batch
                    .column(index(name))
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .expect("SHOW BACKENDS projects every column as Utf8")
                    .clone()
            };
            let process_id = column("ProcessId");
            let diagnostic_status = column("DiagnosticStatus");
            let eligible = column("Eligible");
            let build_identity = column("BuildIdentity");
            let compatibility_id = column("NativeCompatibilityId");
            let status_detail = column("StatusDetail");
            for row in 0..chunk.batch.num_rows() {
                rows.push(BarrierRow {
                    process_id: process_id.value(row).to_string(),
                    diagnostic_status: diagnostic_status.value(row).to_string(),
                    eligible: eligible.value(row) == "true",
                    build_identity: build_identity.value(row).to_string(),
                    compatibility_id: compatibility_id.value(row).to_string(),
                    status_detail: status_detail.value(row).to_string(),
                });
            }
        }
        rows
    }

    /// What the frontend's exact refusal of a heartbeat aimed at a retired
    /// process id looks like by the time it reaches `StatusDetail`.
    const REFUSED_RETIRED_HEARTBEAT: &str = "heartbeat rpc failed: status: FailedPrecondition, \
         message: \"heartbeat expected backend process id does not match this backend\"";

    #[test]
    fn a_same_endpoint_replacement_leaves_the_barrier_projection_clean() {
        // A replaced backend permanently plants that refusal in this
        // projection. The frontend keeps heartbeating a retired process id for
        // as long as its announce lease lives, the process that now owns the
        // listener refuses it by exact identity, and the text lands in
        // `BackendFacts::last_err` -- which is never pruned and clears only on
        // that same process id's next *successful* heartbeat, something a
        // retired process can never produce again.
        //
        // So the question this settles is not whether the text appears. It is
        // where it lands: on the retired row, which the barrier must not count,
        // and never on the replacement, which it must. Without this the only
        // place the contract is written down is the harness's own two fixtures,
        // which pin the endpoints of the transition and not the state a real
        // replacement is left in.
        //
        // Two retries, because that is the cross-process fixture's
        // `heartbeat_timeout_retries`. It matters: one missed heartbeat sets
        // `last_err` without clearing `exact_identity_verified`, so a row can
        // be Live, Eligible and carrying a detail all at once. That has to be a
        // window the replacement closes rather than the resting state.
        let service = ClusterBackendService::new_transient_for_test(2);
        let endpoints = ["127.0.0.1:9070", "127.0.0.1:9071", "127.0.0.1:9072"];
        let announced = endpoints
            .iter()
            .map(|endpoint| {
                let descriptor = descriptor(endpoint.parse().expect("test endpoint"));
                service
                    .record_announce(descriptor.clone(), BackendReportedState::Running)
                    .expect("announce backend");
                verify(&service, &descriptor);
                descriptor
            })
            .collect::<Vec<_>>();
        assert_eq!(
            barrier_rows(&service)
                .iter()
                .filter(|row| row.is_eligible_live())
                .count(),
            endpoints.len(),
            "a verified 1FE+3BE cluster must satisfy the barrier before anything is replaced"
        );

        // The replaced process's first refused heartbeat. Under two retries
        // this does not yet retire it, which is the ambiguous window.
        let retired = announced[0].process_id().expect("retired process id");
        service.record_heartbeat_failure_with_error(retired, REFUSED_RETIRED_HEARTBEAT);
        let during = barrier_rows(&service);
        let retired_row = during
            .iter()
            .find(|row| row.process_id == retired.to_string())
            .expect("the retired process is retained");
        assert_eq!(
            retired_row.diagnostic_status, "Live",
            "a single missed heartbeat may not retire a backend: loss affects future \
             admission, not an attempt in flight"
        );
        assert!(
            retired_row.eligible,
            "eligibility survives one missed heartbeat by design"
        );
        assert_eq!(
            retired_row.status_detail, REFUSED_RETIRED_HEARTBEAT,
            "the refusal is published as this row's StatusDetail"
        );
        assert_eq!(
            during.iter().filter(|row| row.is_eligible_live()).count(),
            endpoints.len() - 1,
            "the barrier reads a Live row carrying a current error as not countable, so \
             the window is one backend short and must be transient"
        );

        // The replacement announces on the same endpoint and passes its own
        // exact pull, which is what transfers endpoint ownership and supersedes
        // the retired process.
        let replacement = descriptor(endpoints[0].parse().expect("test endpoint"));
        service
            .record_announce(replacement.clone(), BackendReportedState::Running)
            .expect("announce replacement");
        verify(&service, &replacement);

        let after = barrier_rows(&service);
        let live = after
            .iter()
            .filter(|row| row.is_eligible_live())
            .collect::<Vec<_>>();
        assert_eq!(
            live.len(),
            endpoints.len(),
            "the barrier must count the whole cluster again once the replacement is verified"
        );
        assert!(
            live.iter().all(|row| row.status_detail.is_empty()),
            "no backend the barrier counts may carry a status detail"
        );
        assert!(
            live.iter().any(|row| row.process_id
                == replacement
                    .process_id()
                    .expect("replacement process id")
                    .to_string()),
            "the replacement is one of the counted backends"
        );
        let retired_row = after
            .iter()
            .find(|row| row.process_id == retired.to_string())
            .expect("the retired process stays visible for triage");
        assert_ne!(
            retired_row.diagnostic_status, "Live",
            "the retired process must be retained as a non-Live row, not counted"
        );
        assert_eq!(
            retired_row.status_detail, REFUSED_RETIRED_HEARTBEAT,
            "the refusal stays where it explains something: on the process it was aimed at"
        );
    }

    #[test]
    fn show_exposes_orthogonal_facts() {
        let service = ClusterBackendService::new_transient_for_test(1);
        let descriptor = descriptor("127.0.0.1:9070".parse().unwrap());
        service
            .record_announce(descriptor.clone(), BackendReportedState::Running)
            .unwrap();
        verify(&service, &descriptor);
        let columns = service
            .show_backends()
            .unwrap()
            .columns
            .into_iter()
            .map(|column| column.name)
            .collect::<Vec<_>>();
        assert!(columns.contains(&"LeaseValid".to_string()));
        assert!(columns.contains(&"IdentityVerified".to_string()));
        assert!(columns.contains(&"EndpointOwned".to_string()));
        assert!(columns.contains(&"Eligible".to_string()));
        assert!(columns.contains(&"DiagnosticStatus".to_string()));
        assert!(columns.contains(&"NativeCompatibilityId".to_string()));
    }

    #[test]
    fn same_island_different_build_identity_is_eligible() {
        let service = ClusterBackendService::new_transient_for_test(1);
        let descriptor = descriptor_with(
            "127.0.0.1:9070".parse().unwrap(),
            "different-build-for-rollout-diagnostics",
            novarocks_types::NativeCompatibilityId::new([0x71; 32]),
        );
        service
            .record_announce(descriptor.clone(), BackendReportedState::Running)
            .unwrap();
        verify(&service, &descriptor);

        assert_eq!(service.snapshot().unwrap().targets().len(), 1);
        let state = service.state.lock().unwrap();
        let facts = state.processes.values().next().expect("announced backend");
        assert!(matches!(
            &facts.compatibility,
            super::Compatibility::Compatible
        ));
    }

    #[test]
    fn other_island_is_observable_but_never_eligible() {
        let service = ClusterBackendService::new_transient_for_test(1);
        let descriptor = descriptor_with(
            "127.0.0.1:9070".parse().unwrap(),
            "other-island-build",
            novarocks_types::NativeCompatibilityId::new([0x72; 32]),
        );
        service
            .record_announce(descriptor.clone(), BackendReportedState::Running)
            .unwrap();
        let revision_before_heartbeat = service.snapshot().unwrap().revision();
        verify(&service, &descriptor);

        let snapshot = service.snapshot().unwrap();
        assert!(snapshot.targets().is_empty());
        assert!(snapshot.revision() > revision_before_heartbeat);
        let state = service.state.lock().unwrap();
        let facts = state.processes.values().next().expect("announced backend");
        assert!(matches!(
            &facts.compatibility,
            super::Compatibility::OtherIsland { .. }
        ));
        assert_eq!(super::diagnostic_status(facts), "OtherIsland");
        assert!(
            facts
                .compatibility
                .detail()
                .contains("other compatibility island")
        );
    }

    #[test]
    fn repeated_verified_other_island_heartbeat_does_not_advance_revision() {
        let service = ClusterBackendService::new_transient_for_test(1);
        let descriptor = descriptor_with(
            "127.0.0.1:9070".parse().unwrap(),
            "other-island-build",
            novarocks_types::NativeCompatibilityId::new([0x72; 32]),
        );
        service
            .record_announce(descriptor.clone(), BackendReportedState::Running)
            .unwrap();
        verify(&service, &descriptor);
        let revision = service.snapshot().unwrap().revision();

        verify(&service, &descriptor);
        assert_eq!(service.snapshot().unwrap().revision(), revision);
    }
}
