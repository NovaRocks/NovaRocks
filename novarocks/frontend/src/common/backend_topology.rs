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
use std::time::Instant;

use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_execution::task_execution::AdmissionEpochCapability;
use novarocks_proto_codec::membership::{BackendProcessDescriptor, BackendReportedState};
use novarocks_types::BackendProcessId;

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

    /// Wait for a newer non-empty eligible snapshot without extending the
    /// statement's original deadline. This is used only between a fully
    /// aborted pre-ready round and its one permitted replacement round.
    fn wait_for_eligible_after(
        &self,
        revision: u64,
        deadline: Instant,
    ) -> Result<BackendTopologySnapshot, BackendTopologyError>;

    /// Records one successfully acknowledged Stage batch.  `fragment_count`
    /// remains separate from the batch boundary so service-only participants
    /// are visible to lifecycle accounting without inflating fragment counts.
    fn record_successful_stage(&self, backend_idx: usize, fragment_count: usize);

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
        captured_generation: BackendProcessId,
        current_generation: BackendProcessId,
        captured_revision: u64,
        current_revision: u64,
    },
    TargetMissing {
        backend_idx: usize,
        captured_generation: BackendProcessId,
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

/// CLS-R2 boundary: this value travels with the membership authority to the
/// frontend. The metrics surface takes already-counted scalars rather than
/// naming this type, so nothing that stays with the aggregate package depends
/// on it.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BackendTopologyMetricsSnapshot {
    pub entries: usize,
    pub announce_lease_valid: usize,
    pub identity_verified: usize,
    pub reported_running: usize,
    pub reported_draining: usize,
    pub compatibility_compatible: usize,
    pub compatibility_other_island: usize,
    pub compatibility_unknown_or_invalid: usize,
    pub endpoint_owned: usize,
    pub endpoint_unowned: usize,
    pub eligible: usize,
    pub revision: u64,
}

/// Publishes the latest frontend-owned topology counts to the shared process
/// metrics endpoint. A scrape reads this snapshot and never resets it.
///
/// The counts cross into the metrics surface as scalars. The membership owner
/// is the only side that knows how to count registry states, and the metrics
/// surface is the only side that knows the gauge label set, so neither has to
/// name a type belonging to the other.
pub fn publish_backend_topology_metrics(snapshot: BackendTopologyMetricsSnapshot) {
    crate::metrics::publish_backend_topology_metrics(
        snapshot.entries,
        snapshot.announce_lease_valid,
        snapshot.identity_verified,
        snapshot.reported_running,
        snapshot.reported_draining,
        snapshot.compatibility_compatible,
        snapshot.compatibility_other_island,
        snapshot.compatibility_unknown_or_invalid,
        snapshot.endpoint_owned,
        snapshot.endpoint_unowned,
        snapshot.eligible,
        snapshot.revision,
    );
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
        /// The descriptor returned by the backend after it verified the
        /// process id supplied by the frontend.  The caller must compare it
        /// with the announced descriptor before changing eligibility.
        descriptor: BackendProcessDescriptor,
        reported_state: BackendReportedState,
        num_cores: u32,
        admission_epoch_capability: AdmissionEpochCapability,
        now_ms: i64,
    },
    Failed {
        err: String,
    },
}

/// Core-local scheduling metric. Topology accounting is performed by the
/// frontend-owned port at the composition boundary.
pub fn record_successful_stage(_backend_idx: usize, fragment_count: usize) {
    crate::metrics::observe_fragments_scheduled(fragment_count);
}

#[derive(Clone, Debug)]
pub struct LiveBackendTarget {
    backend_idx: usize,
    descriptor: BackendProcessDescriptor,
    admission_epoch_capability: AdmissionEpochCapability,
}

impl LiveBackendTarget {
    pub fn new(
        backend_idx: usize,
        descriptor: BackendProcessDescriptor,
        admission_epoch_capability: AdmissionEpochCapability,
    ) -> Self {
        Self {
            backend_idx,
            descriptor,
            admission_epoch_capability,
        }
    }

    pub const fn backend_idx(&self) -> usize {
        self.backend_idx
    }

    pub fn descriptor(&self) -> &BackendProcessDescriptor {
        &self.descriptor
    }

    pub const fn admission_epoch_capability(&self) -> AdmissionEpochCapability {
        self.admission_epoch_capability
    }

    pub fn process_id(&self) -> Result<BackendProcessId, novarocks_proto_codec::ProtocolError> {
        self.descriptor.process_id()
    }

    pub fn endpoint(&self) -> Result<RuntimeEndpoint, novarocks_proto_codec::ProtocolError> {
        let endpoint = self.descriptor.endpoint()?;
        RuntimeEndpoint::new(endpoint.host(), i32::from(endpoint.port())).map_err(|error| {
            novarocks_proto_codec::ProtocolError::new(
                novarocks_proto_codec::FieldPath::root("backend_process_descriptor")
                    .field("endpoint")
                    .field("host"),
                novarocks_proto_codec::ProtocolErrorKind::InvalidValue,
                format!("backend endpoint is invalid for native transport: {error}"),
            )
        })
    }
}

impl PartialEq for LiveBackendTarget {
    fn eq(&self, other: &Self) -> bool {
        self.backend_idx == other.backend_idx
            && self.descriptor.as_proto() == other.descriptor.as_proto()
            && self.admission_epoch_capability == other.admission_epoch_capability
    }
}

impl Eq for LiveBackendTarget {}

/// An immutable, versioned view of the backend targets available when a
/// request was admitted. The owner is responsible for advancing `revision`
/// for every membership or generation change.
///
/// CLS-R2 boundary: durable membership authority is frontend-owned
/// (`ADR-0013`) and the service that advances `revision` moves there. This
/// observed snapshot stays with the aggregate package because `connector`
/// consumes it; that consumer leaves with CLS-R5.
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
            .map(|index| self.targets[index].clone())
    }
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for target-specific frontend integration and regression coverage."
)]
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

    fn wait_for_eligible_after(
        &self,
        _revision: u64,
        _deadline: Instant,
    ) -> Result<BackendTopologySnapshot, BackendTopologyError> {
        Err(BackendTopologyError::Unavailable {
            message: "backend topology port is not installed".to_string(),
        })
    }

    fn record_successful_stage(&self, _backend_idx: usize, _fragment_count: usize) {}

    fn show_backends(&self) -> Result<crate::runtime::query_result::QueryResult, String> {
        Err("backend topology port is not installed".to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::{BackendTopologyError, BackendTopologySnapshot, LiveBackendTarget};
    use novarocks_proto_codec::lifecycle::QueryControlEndpoint;
    use novarocks_proto_codec::membership::BackendProcessDescriptor;
    use novarocks_types::BackendProcessId;

    fn admission_epoch() -> novarocks_execution::task_execution::AdmissionEpochCapability {
        novarocks_execution::task_execution::AdmissionEpochCapability::try_from_bytes([0x61; 16])
            .expect("nonzero epoch")
    }

    fn descriptor(endpoint: SocketAddr) -> BackendProcessDescriptor {
        BackendProcessDescriptor::new(
            BackendProcessId::new_v7(),
            QueryControlEndpoint::new(endpoint.ip().to_string(), endpoint.port())
                .expect("valid endpoint"),
            "test-deployment",
            "test-build",
            novarocks_types::NativeCompatibilityId::new([0x71; 32]),
        )
        .expect("valid descriptor")
    }

    #[test]
    fn topology_snapshot_sorts_targets_by_backend_id() {
        let endpoint = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9030);
        let snapshot = BackendTopologySnapshot::try_new(
            7,
            vec![
                LiveBackendTarget::new(9, descriptor(endpoint), admission_epoch()),
                LiveBackendTarget::new(2, descriptor(endpoint), admission_epoch()),
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
                    LiveBackendTarget::new(2, descriptor(endpoint), admission_epoch()),
                    LiveBackendTarget::new(2, descriptor(endpoint), admission_epoch()),
                ],
            ),
            Err(BackendTopologyError::DuplicateBackendId { backend_idx: 2 })
        );
    }
}
