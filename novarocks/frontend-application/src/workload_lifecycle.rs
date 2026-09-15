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

//! FE-local serving lifecycle and sanitized management observation.
//!
//! Design: ADR-0121. This owner deliberately has no knowledge of MySQL,
//! Native transport, a background scheduler, or business-root ownership. Its
//! query application owns the serving-admission linearization; this owner
//! retains only sanitized catalog and management observation.

use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use novarocks_query_application::serving_admission::{
    FrontendAdmissionError, FrontendServingAdmission, FrontendServingState,
};
use novarocks_workload_control::{WorkClass, WorkClassTotals, WorkloadObservationHandle};
use serde::Serialize;

/// Closed source labels exposed by the sanitized FE management surface.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum FrontendCatalogSourceMode {
    StaticFile,
    DynamicStateStore,
    ManagedController,
}

impl FrontendCatalogSourceMode {
    pub const fn as_metric_label(self) -> &'static str {
        match self {
            Self::StaticFile => "static_file",
            Self::DynamicStateStore => "dynamic_state_store",
            Self::ManagedController => "managed_controller",
        }
    }
}

/// A sanitized snapshot identity. Catalog names, properties, credential references,
/// and physical attachment identities never enter this type.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct FrontendCatalogSnapshotIdentity {
    pub catalog_count: usize,
    pub digest: String,
}

impl FrontendCatalogSnapshotIdentity {
    pub fn try_new(catalog_count: usize, digest: impl Into<String>) -> Result<Self, String> {
        let digest = digest.into();
        if digest.len() != 16 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(
                "frontend catalog snapshot digest must be a 16-hex short digest".to_string(),
            );
        }
        Ok(Self {
            catalog_count,
            digest,
        })
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct FrontendCatalogCounts {
    pub desired: usize,
    pub ready: usize,
    pub unavailable: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct FrontendCatalogServingSnapshot {
    pub source_mode: Option<FrontendCatalogSourceMode>,
    pub bootstrap_complete: bool,
    pub snapshot: Option<FrontendCatalogSnapshotIdentity>,
    pub counts: FrontendCatalogCounts,
}

impl Default for FrontendCatalogServingSnapshot {
    fn default() -> Self {
        Self {
            source_mode: None,
            bootstrap_complete: false,
            snapshot: None,
            counts: FrontendCatalogCounts::default(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct FrontendActiveWorkloads {
    pub statement: usize,
    pub background: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct FrontendWorkloadTotals {
    pub session: u64,
    pub statement: u64,
    pub background: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct FrontendWorkloadServingSnapshot {
    pub active: FrontendActiveWorkloads,
    /// Sanitized aggregate facts from the local workload authority. This
    /// management projection contains no scope identity or mutation path.
    pub governance: FrontendWorkloadGovernanceSnapshot,
    pub rejected_admissions: FrontendWorkloadTotals,
    pub completed_during_drain: FrontendWorkloadTotals,
    pub deadline_cancelled: FrontendWorkloadTotals,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct FrontendWorkloadGovernanceSnapshot {
    pub root_responsibilities: usize,
    pub preparation: usize,
    pub execution: usize,
    pub waiting_records: usize,
    pub peak_waiting_records: usize,
    pub waiting_bytes: u64,
    pub peak_waiting_bytes: u64,
    pub control_ready: usize,
    pub control_inflight: usize,
    pub resource_limit_bytes: u64,
    pub held_bytes: u64,
    pub peak_held_bytes: u64,
    pub result_credit_held_bytes: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct FrontendDrainServingSnapshot {
    pub started_at_unix_ms: Option<u64>,
    pub deadline_unix_ms: Option<u64>,
    pub elapsed_ms: u64,
}

/// The exact sanitized document returned by `/v1/frontend/state`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct FrontendServingSnapshot {
    pub schema_version: u8,
    pub serving_state: FrontendServingState,
    pub catalog: FrontendCatalogServingSnapshot,
    pub workload: FrontendWorkloadServingSnapshot,
    pub drain: FrontendDrainServingSnapshot,
}

impl FrontendServingSnapshot {
    fn starting() -> Self {
        Self {
            schema_version: 1,
            serving_state: FrontendServingState::Starting,
            catalog: FrontendCatalogServingSnapshot::default(),
            workload: FrontendWorkloadServingSnapshot::default(),
            drain: FrontendDrainServingSnapshot::default(),
        }
    }

    pub fn base_ready(&self) -> bool {
        self.serving_state == FrontendServingState::Ready && self.catalog.bootstrap_complete
    }
}

/// Read-only capability for the FE serving signal surface.
pub trait FrontendServingSnapshotReader: Send + Sync {
    fn frontend_serving_snapshot(&self) -> FrontendServingSnapshot;
}

/// Late-bound reader used to start management observability before the full FE
/// application has been opened. Installing an owner is one-way for the listener.
#[derive(Default)]
pub struct LateBoundFrontendServingSnapshotReader {
    reader: RwLock<Option<Arc<dyn FrontendServingSnapshotReader>>>,
}

impl LateBoundFrontendServingSnapshotReader {
    pub fn install(&self, reader: Arc<dyn FrontendServingSnapshotReader>) -> Result<(), String> {
        let mut slot = self
            .reader
            .write()
            .expect("frontend serving reader lock poisoned");
        if slot.is_some() {
            return Err("frontend serving snapshot reader is already installed".to_string());
        }
        *slot = Some(reader);
        Ok(())
    }
}

impl FrontendServingSnapshotReader for LateBoundFrontendServingSnapshotReader {
    fn frontend_serving_snapshot(&self) -> FrontendServingSnapshot {
        self.reader
            .read()
            .expect("frontend serving reader lock poisoned")
            .as_ref()
            .map_or_else(FrontendServingSnapshot::starting, |reader| {
                reader.frontend_serving_snapshot()
            })
    }
}

/// Read-only management composition of the FE serving lifecycle and the sole
/// workload authority. The lifecycle retains only serving/catalog/drain facts;
/// root admission and cancellation history are observed from WorkloadControl.
pub struct FrontendServingWorkloadSnapshotReader {
    lifecycle: Arc<FrontendServingLifecycle>,
    workload: WorkloadObservationHandle,
}

impl FrontendServingWorkloadSnapshotReader {
    pub fn new(
        lifecycle: Arc<FrontendServingLifecycle>,
        workload: WorkloadObservationHandle,
    ) -> Self {
        Self {
            lifecycle,
            workload,
        }
    }
}

impl FrontendServingSnapshotReader for FrontendServingWorkloadSnapshotReader {
    fn frontend_serving_snapshot(&self) -> FrontendServingSnapshot {
        let mut serving = self.lifecycle.frontend_serving_snapshot();
        serving.workload = frontend_workload_snapshot(
            self.workload.snapshot(),
            serving.workload.rejected_admissions.session,
        );
        serving
    }
}

fn frontend_workload_snapshot(
    workload: novarocks_workload_control::WorkloadSnapshot,
    rejected_sessions: u64,
) -> FrontendWorkloadServingSnapshot {
    let mut active = FrontendActiveWorkloads::default();
    for scope in workload.scopes {
        if scope.parent.is_none() {
            match scope.class {
                WorkClass::Query | WorkClass::Management => active.statement += 1,
                WorkClass::MaterializedView
                | WorkClass::Statistics
                | WorkClass::TableMaintenance => active.background += 1,
            }
        }
    }
    let mut rejected_admissions =
        frontend_totals_from_root(&workload.root_lifecycle.rejected_admissions);
    rejected_admissions.session = rejected_sessions;
    FrontendWorkloadServingSnapshot {
        active,
        governance: FrontendWorkloadGovernanceSnapshot {
            root_responsibilities: workload.root_responsibilities,
            preparation: workload.preparation,
            execution: workload.execution,
            waiting_records: workload.waiting_records,
            peak_waiting_records: workload.peak_waiting_records,
            waiting_bytes: workload.waiting_bytes,
            peak_waiting_bytes: workload.peak_waiting_bytes,
            control_ready: workload.control_ready,
            control_inflight: workload.control_inflight,
            resource_limit_bytes: workload.resource_limit_bytes,
            held_bytes: workload.held_bytes,
            peak_held_bytes: workload.peak_held_bytes,
            result_credit_held_bytes: workload.result_credit_held_bytes,
        },
        rejected_admissions,
        completed_during_drain: frontend_totals_from_root(
            &workload.root_lifecycle.completed_after_admission_closed,
        ),
        deadline_cancelled: frontend_totals_from_root(
            &workload.root_lifecycle.frontend_drain_deadline_cancelled,
        ),
    }
}

fn frontend_totals_from_root(totals: &WorkClassTotals) -> FrontendWorkloadTotals {
    FrontendWorkloadTotals {
        session: 0,
        statement: totals.query.saturating_add(totals.management),
        background: totals
            .materialized_view
            .saturating_add(totals.statistics)
            .saturating_add(totals.table_maintenance),
    }
}

struct Inner {
    catalog: FrontendCatalogServingSnapshot,
}

impl Default for Inner {
    fn default() -> Self {
        Self {
            catalog: FrontendCatalogServingSnapshot::default(),
        }
    }
}

struct LifecycleShared {
    inner: Mutex<Inner>,
}

/// Role-local management observation around the query application's admission owner.
// Design: ADR-0121 (docs/adr/ADR-0121-frontend-serving-lifecycle-and-admission-drain.md)
#[derive(Clone)]
pub struct FrontendServingLifecycle {
    admission: FrontendServingAdmission,
    shared: Arc<LifecycleShared>,
}

impl Default for FrontendServingLifecycle {
    fn default() -> Self {
        Self::new()
    }
}

impl FrontendServingLifecycle {
    pub fn new() -> Self {
        let lifecycle = Self {
            admission: FrontendServingAdmission::new(),
            shared: Arc::new(LifecycleShared {
                inner: Mutex::new(Inner::default()),
            }),
        };
        lifecycle.publish_metrics();
        lifecycle
    }

    /// The query application's exact session-admission owner.
    pub fn admission(&self) -> FrontendServingAdmission {
        self.admission.clone()
    }

    /// Publishes sanitized catalog bootstrap facts. The caller owns catalog
    /// materialization; this lifecycle merely owns aggregate observation.
    pub fn publish_catalog_bootstrap(
        &self,
        source_mode: FrontendCatalogSourceMode,
        bootstrap_complete: bool,
        snapshot: Option<FrontendCatalogSnapshotIdentity>,
        counts: FrontendCatalogCounts,
    ) {
        let mut inner = self
            .shared
            .inner
            .lock()
            .expect("frontend lifecycle lock poisoned");
        inner.catalog = FrontendCatalogServingSnapshot {
            source_mode: Some(source_mode),
            bootstrap_complete,
            snapshot,
            counts,
        };
        drop(inner);
        self.publish_metrics();
    }

    /// Transitions only from Starting to Ready after the bootstrap owner has
    /// completed its exact snapshot/materialization barrier.
    pub fn mark_ready(&self) -> Result<(), FrontendAdmissionError> {
        self.admission.mark_ready()?;
        self.publish_metrics();
        Ok(())
    }

    /// Atomically closes admission and records the one-way drain deadline.
    /// Repeated calls preserve the original deadline and are idempotent.
    pub fn begin_drain(&self, timeout: Duration) -> FrontendServingState {
        let state = self.admission.begin_drain(timeout);
        self.publish_metrics();
        state
    }

    /// Marks final teardown without creating a path back to Ready.
    pub fn mark_stopping(&self) {
        self.admission.mark_stopping();
        self.publish_metrics();
    }

    fn publish_metrics(&self) {
        crate::metrics::publish_frontend_serving_metrics(self.frontend_serving_snapshot());
    }
}

impl FrontendServingSnapshotReader for FrontendServingLifecycle {
    fn frontend_serving_snapshot(&self) -> FrontendServingSnapshot {
        let inner = self
            .shared
            .inner
            .lock()
            .expect("frontend lifecycle lock poisoned");
        let admission = self.admission.snapshot();
        let now = SystemTime::now();
        FrontendServingSnapshot {
            schema_version: 1,
            serving_state: admission.state,
            catalog: inner.catalog.clone(),
            workload: FrontendWorkloadServingSnapshot {
                active: FrontendActiveWorkloads::default(),
                governance: FrontendWorkloadGovernanceSnapshot::default(),
                rejected_admissions: FrontendWorkloadTotals {
                    session: admission.rejected_sessions,
                    ..FrontendWorkloadTotals::default()
                },
                completed_during_drain: FrontendWorkloadTotals::default(),
                deadline_cancelled: FrontendWorkloadTotals::default(),
            },
            drain: FrontendDrainServingSnapshot {
                started_at_unix_ms: admission.drain_started_at.and_then(unix_millis),
                deadline_unix_ms: admission.drain_deadline.and_then(unix_millis),
                elapsed_ms: admission
                    .drain_started_at
                    .and_then(|started| now.duration_since(started).ok())
                    .map_or(0, |elapsed| {
                        elapsed.as_millis().min(u64::MAX as u128) as u64
                    }),
            },
        }
    }
}

fn unix_millis(time: SystemTime) -> Option<u64> {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use novarocks_workload_control::{
        CancellationReason, ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };

    use super::*;

    #[test]
    fn serving_state_and_governed_workload_observation_share_one_drain_boundary() {
        let lifecycle = Arc::new(FrontendServingLifecycle::new());
        lifecycle.mark_ready().expect("mark ready");
        let control = WorkloadControl::try_new_split(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 128,
                control_bytes: 16,
                per_scope_bytes: 112,
            },
        )
        .expect("valid workload control")
        .owner;
        control.mark_ready().expect("workload ready");
        let reader = FrontendServingWorkloadSnapshotReader::new(
            Arc::clone(&lifecycle),
            control.observation(),
        );
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .expect("admit query root");
        assert_eq!(
            reader.frontend_serving_snapshot().workload.active.statement,
            1
        );
        let initial_governance = &reader.frontend_serving_snapshot().workload.governance;
        assert_eq!(initial_governance.resource_limit_bytes, 128);
        assert_eq!(initial_governance.held_bytes, 0);
        assert_eq!(initial_governance.peak_held_bytes, 0);
        assert_eq!(initial_governance.result_credit_held_bytes, 0);

        assert_eq!(
            lifecycle.begin_drain(Duration::from_secs(1)),
            FrontendServingState::Draining
        );
        control.close_admission();
        assert!(matches!(
            control.try_begin_root(WorkRequest::new(WorkClass::Management)),
            Err(novarocks_workload_control::WorkError::Closed)
        ));
        assert_eq!(
            control.cancel_active_roots(CancellationReason::FrontendDrainDeadlineExceeded),
            1
        );
        drop(work.business);
        work.owner.complete();

        let snapshot = reader.frontend_serving_snapshot();
        assert_eq!(
            snapshot.workload.active.statement, 1,
            "a requested cancellation retains its real root responsibility until control settles"
        );
        assert_eq!(snapshot.workload.rejected_admissions.statement, 1);
        assert_eq!(snapshot.workload.completed_during_drain.statement, 1);
        assert_eq!(snapshot.workload.deadline_cancelled.statement, 1);
    }

    #[test]
    fn session_registration_rejects_after_drain() {
        let lifecycle = FrontendServingLifecycle::new();
        lifecycle.mark_ready().expect("mark ready");
        lifecycle.begin_drain(Duration::from_secs(1));
        assert_eq!(
            lifecycle.admission().register_session(|| 7),
            Err(FrontendAdmissionError::Draining)
        );
    }

    #[test]
    fn only_starting_can_become_ready_and_drain_is_idempotent() {
        let lifecycle = FrontendServingLifecycle::new();
        lifecycle.mark_ready().expect("mark ready");
        lifecycle.begin_drain(Duration::from_secs(2));
        lifecycle.begin_drain(Duration::from_secs(30));
        assert_eq!(
            lifecycle.mark_ready(),
            Err(FrontendAdmissionError::Draining)
        );
        let snapshot = lifecycle.frontend_serving_snapshot();
        assert_eq!(snapshot.serving_state, FrontendServingState::Draining);
        assert!(snapshot.drain.started_at_unix_ms.is_some());
        assert!(snapshot.drain.deadline_unix_ms.is_some());
    }

    #[test]
    fn snapshot_identity_rejects_non_sanitized_digest() {
        assert!(FrontendCatalogSnapshotIdentity::try_new(1, "0123456789abcdef").is_ok());
        assert!(FrontendCatalogSnapshotIdentity::try_new(1, "catalog-name").is_err());
    }

    #[test]
    fn late_bound_reader_stays_starting_until_the_owner_is_installed() {
        let reader = LateBoundFrontendServingSnapshotReader::default();
        assert_eq!(
            reader.frontend_serving_snapshot().serving_state,
            FrontendServingState::Starting
        );
        let lifecycle = Arc::new(FrontendServingLifecycle::new());
        lifecycle.mark_ready().expect("mark ready");
        reader.install(lifecycle).expect("install lifecycle reader");
        assert_eq!(
            reader.frontend_serving_snapshot().serving_state,
            FrontendServingState::Ready
        );
        assert!(
            reader
                .install(Arc::new(FrontendServingLifecycle::new()))
                .is_err()
        );
    }
}
