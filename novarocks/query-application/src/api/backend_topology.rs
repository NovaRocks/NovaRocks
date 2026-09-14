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

//! Query-application backend topology and lifecycle boundary.

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use novarocks_execution_contract::{
    AdmissionEpochCapability, BackendProcessDescriptor, BackendReportedState, RuntimeEndpoint,
};
use novarocks_types::BackendProcessId;

/// Frontend-owned topology and backend-management boundary consumed by core.
///
/// Core intentionally has no registry singleton, heartbeat loop, or role-aware
/// backend-management implementation. Composition roots inject this port.
pub trait BackendTopologyPort: Send + Sync + 'static {
    fn snapshot(&self) -> Result<BackendTopologySnapshot, BackendTopologyError>;

    /// Subscribes to exact topology revision changes. Consumers must re-read
    /// [`Self::snapshot`] after every notification; the revision is only a
    /// wake-up hint and never a schedulable topology by itself.
    fn subscribe_changes(&self) -> tokio::sync::watch::Receiver<u64>;

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
}

pub type BackendTopologyService = Arc<dyn BackendTopologyPort>;
pub type BeId = u32;

/// Narrow live process-lifecycle evidence source for residual convergence.
/// It has no snapshot, placement, or successor-scheduling capability.
pub trait BackendProcessObservationPort: Send + Sync + 'static {
    fn subscribe_process_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        let (_, receiver) = tokio::sync::watch::channel(0);
        receiver
    }

    /// Observes the lifecycle relationship between one frozen process and the
    /// exact endpoint it owned when the attempt was prepared. A missing or
    /// heartbeat-unobservable process is not replacement evidence.
    fn observe_process_at_endpoint(
        &self,
        expected_process: BackendProcessId,
        expected_endpoint: &RuntimeEndpoint,
    ) -> Result<BackendProcessObservation, BackendTopologyError>;
}

pub type BackendProcessObservationService = Arc<dyn BackendProcessObservationPort>;

/// Live topology evidence relevant to residual attempt convergence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendProcessObservation {
    /// The frozen process still owns the endpoint and is currently observable.
    Current,
    /// No exact positive lifecycle fact is available yet.
    Unobservable,
    /// A different, exact process identity now owns the frozen endpoint.
    Replaced { current_process: BackendProcessId },
}

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

    pub const fn process_id(&self) -> Result<BackendProcessId, BackendTopologyError> {
        Ok(self.descriptor.process_id())
    }

    pub fn endpoint(&self) -> Result<RuntimeEndpoint, BackendTopologyError> {
        Ok(self.descriptor.endpoint().clone())
    }
}

impl PartialEq for LiveBackendTarget {
    fn eq(&self, other: &Self) -> bool {
        self.backend_idx == other.backend_idx
            && self.descriptor == other.descriptor
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

    fn subscribe_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        let (_, receiver) = tokio::sync::watch::channel(0);
        receiver
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
}

#[cfg(test)]
impl BackendProcessObservationPort for NoopBackendTopologyPort {
    fn observe_process_at_endpoint(
        &self,
        _expected_process: BackendProcessId,
        _expected_endpoint: &RuntimeEndpoint,
    ) -> Result<BackendProcessObservation, BackendTopologyError> {
        Ok(BackendProcessObservation::Unobservable)
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::{BackendTopologyError, BackendTopologySnapshot, LiveBackendTarget};
    use novarocks_execution_contract::AdmissionEpochCapability;
    use novarocks_execution_contract::{BackendProcessDescriptor, RuntimeEndpoint};
    use novarocks_types::BackendProcessId;

    fn admission_epoch() -> AdmissionEpochCapability {
        AdmissionEpochCapability::try_from_bytes([0x61; 16]).expect("nonzero epoch")
    }

    fn descriptor(endpoint: SocketAddr) -> BackendProcessDescriptor {
        BackendProcessDescriptor::try_new(
            BackendProcessId::new_v7(),
            RuntimeEndpoint::new(endpoint.ip().to_string(), i32::from(endpoint.port()))
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
