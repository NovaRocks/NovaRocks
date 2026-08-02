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

//! Neutral frontend-facing backend topology and lifecycle boundary.

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::runtime::endpoint::RuntimeEndpoint;

/// Frontend-owned topology and backend-management boundary consumed by core.
///
/// Core intentionally has no registry singleton, heartbeat loop, or role-aware
/// backend-management implementation. Composition roots inject this port.
pub trait BackendTopologyPort: Send + Sync + 'static {
    fn snapshot(&self) -> Result<BackendTopologySnapshot, BackendTopologyError>;

    fn validate_snapshot(
        &self,
        expected: &BackendTopologySnapshot,
    ) -> Result<(), BackendTopologyValidationError>;

    /// Records one successfully acknowledged Stage batch.  `fragment_count`
    /// remains separate from the batch boundary so service-only participants
    /// are visible to lifecycle accounting without inflating fragment counts.
    fn record_successful_stage(&self, backend_idx: usize, fragment_count: usize);

    fn add_backend(&self, endpoint: SocketAddr) -> Result<(), String>;

    fn drop_backend(&self, endpoint: SocketAddr, force: bool) -> Result<(), String>;

    fn show_backends(&self) -> Result<crate::runtime::query_result::QueryResult, String>;
}

pub type BackendTopologyService = Arc<dyn BackendTopologyPort>;
pub type BeId = u32;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BackendTopologyError {
    DuplicateBackendId { backend_idx: usize },
    RevisionExhausted,
    Unavailable { message: String },
}

impl fmt::Display for BackendTopologyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateBackendId { backend_idx } => {
                write!(
                    f,
                    "backend topology snapshot contains duplicate backend id {backend_idx}"
                )
            }
            Self::RevisionExhausted => write!(f, "backend topology revision space is exhausted"),
            Self::Unavailable { message } => {
                write!(f, "backend topology is unavailable: {message}")
            }
        }
    }
}

impl std::error::Error for BackendTopologyError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BackendTopologyValidationError {
    RevisionChanged {
        captured_revision: u64,
        current_revision: u64,
    },
    GenerationChanged {
        backend_idx: usize,
        captured_generation: u64,
        current_generation: u64,
        captured_revision: u64,
        current_revision: u64,
    },
    TargetMissing {
        backend_idx: usize,
        captured_generation: u64,
        captured_revision: u64,
        current_revision: u64,
    },
    ContentChangedWithoutRevision {
        revision: u64,
    },
    Unavailable(BackendTopologyError),
}

impl fmt::Display for BackendTopologyValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RevisionChanged {
                captured_revision,
                current_revision,
            } => write!(
                f,
                "backend topology changed: captured revision {captured_revision}, current revision {current_revision}"
            ),
            Self::GenerationChanged {
                backend_idx,
                captured_generation,
                current_generation,
                captured_revision,
                current_revision,
            } => write!(
                f,
                "backend {backend_idx} generation changed: captured {captured_generation} at revision {captured_revision}, current {current_generation} at revision {current_revision}"
            ),
            Self::TargetMissing {
                backend_idx,
                captured_generation,
                captured_revision,
                current_revision,
            } => write!(
                f,
                "backend {backend_idx} generation {captured_generation} from revision {captured_revision} is no longer live at revision {current_revision}"
            ),
            Self::ContentChangedWithoutRevision { revision } => write!(
                f,
                "backend topology content changed without a revision advance at revision {revision}"
            ),
            Self::Unavailable(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for BackendTopologyValidationError {}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BackendTopologyMetricsSnapshot {
    pub registering: usize,
    pub live: usize,
    pub lost: usize,
    pub decommissioning: usize,
}

/// Publishes the latest frontend-owned topology counts to the shared process
/// metrics endpoint. A scrape reads this snapshot and never resets it.
pub fn publish_backend_topology_metrics(snapshot: BackendTopologyMetricsSnapshot) {
    crate::service::metrics_http::publish_backend_topology_metrics(snapshot);
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveBackendSnapshot {
    entries: Vec<(usize, SocketAddr)>,
}

impl LiveBackendSnapshot {
    pub fn new(entries: Vec<(usize, SocketAddr)>) -> Self {
        Self { entries }
    }

    pub fn from_endpoints(backends: Vec<SocketAddr>) -> Self {
        Self::new(backends.into_iter().enumerate().collect())
    }

    pub fn entries(&self) -> &[(usize, SocketAddr)] {
        &self.entries
    }
}

#[derive(Clone, Debug)]
pub enum HeartbeatOutcome {
    Ok {
        start_epoch: u64,
        version: String,
        num_cores: u32,
        now_ms: i64,
    },
    Failed {
        err: String,
    },
}

/// Core-local scheduling metric. Topology accounting is performed by the
/// frontend-owned port at the composition boundary.
pub fn record_successful_stage(_backend_idx: usize, fragment_count: usize) {
    crate::service::metrics_http::observe_fragments_scheduled(fragment_count);
}

/// Resolves the report endpoint after the coordinator gRPC listener has bound.
///
/// A configured port of zero requests an ephemeral listener, so its actual
/// bound port must be read at query time rather than frozen during host open.
pub struct CoordinatorReportEndpoint {
    endpoint: RuntimeEndpoint,
}

impl CoordinatorReportEndpoint {
    pub fn new(host: impl Into<String>, port: u16) -> Result<Self, String> {
        Ok(Self {
            endpoint: RuntimeEndpoint::new(host, i32::from(port))?,
        })
    }

    pub fn from_socket_addr(endpoint: SocketAddr) -> Self {
        Self {
            endpoint: RuntimeEndpoint::from_socket_addr(endpoint),
        }
    }

    pub(crate) fn into_runtime_endpoint(self) -> RuntimeEndpoint {
        self.endpoint
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BackendQueryEvent {
    Unavailable {
        backend_idx: usize,
        reason: String,
    },
    Restarted {
        backend_idx: usize,
        old_epoch: u64,
        new_epoch: u64,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LiveBackendTarget {
    backend_idx: usize,
    endpoint: SocketAddr,
    start_epoch: u64,
}

impl LiveBackendTarget {
    pub fn new(backend_idx: usize, endpoint: SocketAddr, start_epoch: u64) -> Self {
        Self {
            backend_idx,
            endpoint,
            start_epoch,
        }
    }

    pub const fn backend_idx(self) -> usize {
        self.backend_idx
    }

    pub const fn endpoint(self) -> SocketAddr {
        self.endpoint
    }

    pub const fn start_epoch(self) -> u64 {
        self.start_epoch
    }
}

/// An immutable, versioned view of the backend targets available when a
/// request was admitted. The owner is responsible for advancing `revision`
/// for every membership or generation change.
// Design: ADR-0011 (docs/adr/ADR-0011-immutable-request-execution-context.md)
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendTopologySnapshot {
    revision: u64,
    targets: Arc<[LiveBackendTarget]>,
}

impl BackendTopologySnapshot {
    pub fn try_new(
        revision: u64,
        mut targets: Vec<LiveBackendTarget>,
    ) -> Result<Self, BackendTopologyError> {
        targets.sort_by_key(|target| target.backend_idx());
        for pair in targets.windows(2) {
            if pair[0].backend_idx() == pair[1].backend_idx() {
                return Err(BackendTopologyError::DuplicateBackendId {
                    backend_idx: pair[0].backend_idx(),
                });
            }
        }
        Ok(Self {
            revision,
            targets: targets.into(),
        })
    }

    pub fn empty(revision: u64) -> Self {
        Self {
            revision,
            targets: Arc::from([]),
        }
    }

    pub const fn revision(&self) -> u64 {
        self.revision
    }

    pub fn targets(&self) -> &[LiveBackendTarget] {
        &self.targets
    }

    pub fn target(&self, backend_idx: usize) -> Option<LiveBackendTarget> {
        self.targets
            .binary_search_by_key(&backend_idx, |target| target.backend_idx())
            .ok()
            .map(|index| self.targets[index])
    }
}

/// Frontend-owned query activity consumed by the core backend registry.
///
/// The sink owns query-wide failure and exact remote cancellation. Core only
/// forwards lifecycle facts and never performs BE-local query cleanup.
pub trait BackendQueryEventSink: Send + Sync + 'static {
    fn on_backend_event(&self, event: BackendQueryEvent);

    fn backend_has_active_queries(&self, backend_idx: usize) -> bool;

    fn replace_live_backends(&self, revision: u64, backends: Vec<LiveBackendTarget>);
}

pub trait CoordinatorReportEndpointSink: Send + Sync + 'static {
    fn set_bound_port(&self, port: u16);
}

#[cfg(test)]
pub(crate) struct NoopBackendQueryEventSink;

#[cfg(test)]
impl BackendQueryEventSink for NoopBackendQueryEventSink {
    fn on_backend_event(&self, _event: BackendQueryEvent) {}

    fn backend_has_active_queries(&self, _backend_idx: usize) -> bool {
        false
    }

    fn replace_live_backends(&self, _revision: u64, _backends: Vec<LiveBackendTarget>) {}
}

#[cfg(test)]
pub(crate) struct NoopCoordinatorReportEndpointSink;

#[cfg(test)]
impl CoordinatorReportEndpointSink for NoopCoordinatorReportEndpointSink {
    fn set_bound_port(&self, _port: u16) {}
}

#[cfg(test)]
pub(crate) struct NoopBackendTopologyPort;

#[cfg(test)]
impl BackendTopologyPort for NoopBackendTopologyPort {
    fn snapshot(&self) -> Result<BackendTopologySnapshot, BackendTopologyError> {
        Ok(BackendTopologySnapshot::empty(0))
    }

    fn validate_snapshot(
        &self,
        expected: &BackendTopologySnapshot,
    ) -> Result<(), BackendTopologyValidationError> {
        let current = self
            .snapshot()
            .map_err(BackendTopologyValidationError::Unavailable)?;
        if current == *expected {
            Ok(())
        } else {
            Err(
                BackendTopologyValidationError::ContentChangedWithoutRevision {
                    revision: expected.revision(),
                },
            )
        }
    }

    fn record_successful_stage(&self, _backend_idx: usize, _fragment_count: usize) {}

    fn add_backend(&self, _endpoint: SocketAddr) -> Result<(), String> {
        Err("backend topology port is not installed".to_string())
    }

    fn drop_backend(&self, _endpoint: SocketAddr, _force: bool) -> Result<(), String> {
        Err("backend topology port is not installed".to_string())
    }

    fn show_backends(&self) -> Result<crate::runtime::query_result::QueryResult, String> {
        Err("backend topology port is not installed".to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::{
        BackendTopologyError, BackendTopologySnapshot, CoordinatorReportEndpoint, LiveBackendTarget,
    };

    #[test]
    fn coordinator_report_endpoint_accepts_advertised_dns_hostnames() {
        CoordinatorReportEndpoint::new("frontend.internal", 19070)
            .expect("advertised DNS hostname is a valid same-wire endpoint");
    }

    #[test]
    fn topology_snapshot_sorts_targets_by_backend_id() {
        let endpoint = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9030);
        let snapshot = BackendTopologySnapshot::try_new(
            7,
            vec![
                LiveBackendTarget::new(9, endpoint, 1),
                LiveBackendTarget::new(2, endpoint, 3),
            ],
        )
        .expect("distinct targets form a snapshot");

        assert_eq!(snapshot.revision(), 7);
        assert_eq!(
            snapshot
                .targets()
                .iter()
                .map(|target| target.backend_idx())
                .collect::<Vec<_>>(),
            vec![2, 9]
        );
    }

    #[test]
    fn topology_snapshot_rejects_duplicate_backend_ids() {
        let endpoint = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9030);
        assert_eq!(
            BackendTopologySnapshot::try_new(
                7,
                vec![
                    LiveBackendTarget::new(2, endpoint, 1),
                    LiveBackendTarget::new(2, endpoint, 2),
                ],
            ),
            Err(BackendTopologyError::DuplicateBackendId { backend_idx: 2 })
        );
    }
}
