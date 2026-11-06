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

//! Worker-owned runtime inputs for typed connector scans.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use novarocks_execution::connector::SplitQueues;
use novarocks_execution::runtime::mem_tracker::MemTracker;
use novarocks_execution::runtime_filter::RuntimeFilterSessionRef;
use novarocks_spi::connector::read_stack::runtime::ConnectorReadAssignment;
use novarocks_spi::connector::read_stack::{
    ConnectorReadDynamicFilter, ConnectorReadTableHandle, ConnectorSession,
};
use novarocks_spi::connector::{
    CatalogHandle, ConnectorError, ConnectorErrorKind, ConnectorExecutionReadBinding,
    ConnectorExecutionResources, ConnectorExecutionWriteBinding, ConnectorReservationBackend,
    ConnectorResourceCheckpoint, ConnectorResourceClass, ConnectorResourceLedger,
    ConnectorStorageResolver,
};
use novarocks_types::QueryExecutionId;

use crate::read_attempt::{ReceivedReadSplit, TaskReadBindingRegistry};

/// Looks up an attempt's runtime-filter session at the moment a scan binds it.
pub type RuntimeFilterSessionResolver = Arc<
    dyn Fn() -> Result<Option<RuntimeFilterSessionRef>, crate::RuntimeFilterContractError>
        + Send
        + Sync,
>;

/// Absence at the query-leased execution lookup, not proof of cancellation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogExecutionAccessError {
    LeaseUnavailable { catalog: CatalogHandle },
    ReadUnavailable { catalog: CatalogHandle },
    WriteUnavailable { catalog: CatalogHandle },
}
impl std::fmt::Display for CatalogExecutionAccessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LeaseUnavailable { catalog } => write!(
                f,
                "no query-leased catalog runtime exists for {}@{}",
                catalog.catalog_name().as_str(),
                catalog.version().short_hex()
            ),
            Self::ReadUnavailable { catalog } => write!(
                f,
                "catalog runtime for {}@{} has no typed read capability",
                catalog.catalog_name().as_str(),
                catalog.version().short_hex()
            ),
            Self::WriteUnavailable { catalog } => write!(
                f,
                "catalog runtime for {}@{} has no typed write capability",
                catalog.catalog_name().as_str(),
                catalog.version().short_hex()
            ),
        }
    }
}
impl std::error::Error for CatalogExecutionAccessError {}
impl novarocks_execution_contract::failure::FailureProjection for CatalogExecutionAccessError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use crate::failure_reason::WorkerFailureReason as R;
        use novarocks_execution_contract::failure::*;
        FailureFact::new(
            FailureClass::Dependency,
            match self {
                Self::LeaseUnavailable { .. } => R::CatalogExecutionLeaseUnavailable,
                Self::ReadUnavailable { .. } => R::CatalogExecutionReadUnavailable,
                Self::WriteUnavailable { .. } => R::CatalogExecutionWriteUnavailable,
            }
            .reason(),
        )
    }
}

/// Resolves one reader only through an attempt's query-leased immutable catalog
/// runtime. It intentionally cannot reinstall a provider runtime.
pub type CatalogReadExecutionResolver = Arc<
    dyn Fn(&CatalogHandle) -> Result<ConnectorExecutionReadBinding, CatalogExecutionAccessError>
        + Send
        + Sync,
>;

/// Resolves one writer only through an attempt's query-leased immutable catalog
/// runtime.
pub type CatalogWriteExecutionResolver = Arc<
    dyn Fn(&CatalogHandle) -> Result<ConnectorExecutionWriteBinding, CatalogExecutionAccessError>
        + Send
        + Sync,
>;

/// The SPI-only facts a typed reader retains after native carrier decoding.
///
/// The protocol adapter constructs this value after it has validated and
/// decoded the carrier. Worker execution deliberately receives neither a DTO
/// nor a protocol decoder, so a reader cannot reinterpret or revalidate wire
/// semantics after admission.
#[derive(Clone)]
pub struct TypedConnectorReadDescriptor {
    table: ConnectorReadTableHandle,
    assignments: Vec<ConnectorReadAssignment>,
    complete_dynamic_filter: Arc<ConnectorReadDynamicFilter>,
}

impl TypedConnectorReadDescriptor {
    pub fn new(
        table: ConnectorReadTableHandle,
        assignments: Vec<ConnectorReadAssignment>,
        complete_dynamic_filter: Arc<ConnectorReadDynamicFilter>,
    ) -> Self {
        Self {
            table,
            assignments,
            complete_dynamic_filter,
        }
    }

    pub const fn table(&self) -> &ConnectorReadTableHandle {
        &self.table
    }

    pub fn assignments(&self) -> &[ConnectorReadAssignment] {
        &self.assignments
    }

    pub fn complete_dynamic_filter(&self) -> Arc<ConnectorReadDynamicFilter> {
        Arc::clone(&self.complete_dynamic_filter)
    }
}

/// Adapts connector reservations to the exact fragment tracker installed by
/// task admission. Decode may construct the runtime before admission, but no
/// connector can reserve memory until the host installs that tracker.
struct WorkerConnectorResourceLedger {
    tracker: OnceLock<Arc<MemTracker>>,
    checkpoint: AtomicU64,
}

impl WorkerConnectorResourceLedger {
    fn new() -> Self {
        Self {
            tracker: OnceLock::new(),
            checkpoint: AtomicU64::new(0),
        }
    }

    fn install(&self, tracker: Arc<MemTracker>) -> Result<(), ConnectorResourceBindingError> {
        if let Some(existing) = self.tracker.get() {
            return if Arc::ptr_eq(existing, &tracker) {
                Ok(())
            } else {
                Err(ConnectorResourceBindingError::TrackerRebound)
            };
        }
        self.tracker
            .set(tracker)
            .map_err(|_| ConnectorResourceBindingError::InstallationRaced)
    }

    fn admitted_resources(
        self: &Arc<Self>,
        tracker: &Arc<MemTracker>,
    ) -> Result<ConnectorExecutionResources, ConnectorResourceBindingError> {
        let installed = self
            .tracker
            .get()
            .ok_or(ConnectorResourceBindingError::BeforeAdmission)?;
        if !Arc::ptr_eq(installed, tracker) {
            return Err(ConnectorResourceBindingError::WrongTracker);
        }
        Ok(ConnectorExecutionResources::from_admitted_ledger(
            Arc::clone(self) as Arc<dyn ConnectorResourceLedger>,
        ))
    }
}

struct WorkerConnectorReservation {
    tracker: Arc<MemTracker>,
    bytes: i64,
}

impl ConnectorReservationBackend for WorkerConnectorReservation {
    fn bytes(&self) -> u64 {
        u64::try_from(self.bytes).expect("connector resource lease bytes are non-negative")
    }

    fn try_grow(&mut self, additional: u64) -> Result<(), ConnectorError> {
        let additional = i64::try_from(additional).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector reservation exceeds the native memory tracker range",
            )
        })?;
        if let Err(error) = self.tracker.consume_and_check_limit(additional) {
            self.tracker.release(additional);
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                error,
            ));
        }
        self.bytes = self.bytes.checked_add(additional).ok_or_else(|| {
            self.tracker.release(additional);
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "connector resource lease overflowed",
            )
        })?;
        Ok(())
    }

    fn shrink_to(&mut self, bytes: u64) -> Result<(), ConnectorError> {
        let bytes = i64::try_from(bytes).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector resource lease target exceeds the native memory tracker range",
            )
        })?;
        if bytes > self.bytes {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector resource lease cannot grow through shrink_to",
            ));
        }
        self.tracker.release(self.bytes - bytes);
        self.bytes = bytes;
        Ok(())
    }
}

impl Drop for WorkerConnectorReservation {
    fn drop(&mut self) {
        self.tracker.release(self.bytes);
    }
}

impl ConnectorResourceLedger for WorkerConnectorResourceLedger {
    fn checkpoint(&self) -> Result<ConnectorResourceCheckpoint, ConnectorError> {
        Ok(ConnectorResourceCheckpoint::new(
            self.checkpoint.fetch_add(1, Ordering::Relaxed),
        ))
    }

    fn try_reserve(
        &self,
        _class: ConnectorResourceClass,
        bytes: u64,
    ) -> Result<Box<dyn ConnectorReservationBackend>, ConnectorError> {
        let tracker = self.tracker.get().cloned().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector attempted to reserve runtime memory before native fragment admission",
            )
        })?;
        let bytes = i64::try_from(bytes).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector reservation exceeds the native memory tracker range",
            )
        })?;
        if let Err(error) = tracker.consume_and_check_limit(bytes) {
            tracker.release(bytes);
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                error,
            ));
        }
        Ok(Box::new(WorkerConnectorReservation { tracker, bytes }))
    }
}

/// Everything a typed connector scan needs that only its task runtime can
/// supply: installed provider generation, attempt split queues, and the
/// role-local session. Bundling this keeps decode from accumulating authority.
#[derive(Clone)]
pub struct TypedScanRuntime {
    execution_id: QueryExecutionId,
    catalog_read_execution: CatalogReadExecutionResolver,
    catalog_write_execution: CatalogWriteExecutionResolver,
    queues: Arc<SplitQueues<ReceivedReadSplit>>,
    session: ConnectorSession,
    runtime_filter: RuntimeFilterSessionResolver,
    read_context: Arc<TaskReadBindingRegistry>,
    storage_resolver: Arc<dyn ConnectorStorageResolver>,
    connector_resource_ledger: Arc<WorkerConnectorResourceLedger>,
    stream_host: ScanStreamHost,
}

/// What hosts the typed scan streams of one BE application: the bounds of
/// each stream's speculative successor window, the one timer that supervises
/// paused windows, and the scan I/O runtime a stream is entered into whenever
/// it is polled or closed. That context supplies timers and I/O drivers;
/// entering it grants no spawn authority. Concurrent query work goes through
/// the admitted connector operation and I/O entries.
#[derive(Clone)]
pub struct ScanStreamHost {
    preparation: ScanPreparationConfig,
    timer: Arc<crate::ScanPreparationTimer>,
    runtime: tokio::runtime::Handle,
}

impl ScanStreamHost {
    /// Starts the host's preparation timer; the caller keeps one host per BE
    /// application.
    pub fn new(preparation: ScanPreparationConfig, runtime: tokio::runtime::Handle) -> Self {
        Self {
            preparation,
            timer: crate::ScanPreparationTimer::new(),
            runtime,
        }
    }

    pub const fn preparation(&self) -> ScanPreparationConfig {
        self.preparation
    }

    pub fn timer(&self) -> &Arc<crate::ScanPreparationTimer> {
        &self.timer
    }

    pub(crate) fn runtime(&self) -> &tokio::runtime::Handle {
        &self.runtime
    }
    #[cfg(any(test, novarocks_dev_controls))]
    pub fn runtime_for_test(&self) -> &tokio::runtime::Handle {
        self.runtime()
    }
}

/// BE-local bounds for one typed scan's speculative successor window.
/// These bounds do not restrict demand reads or the shared physical range service.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScanPreparationConfig {
    pub input_bytes_per_stream: usize,
    pub max_candidates: usize,
    pub pause_release: std::time::Duration,
    pub rearm: std::time::Duration,
    pub progress_bucket: std::time::Duration,
}

impl ScanPreparationConfig {
    pub fn try_new(
        input_bytes_per_stream: usize,
        max_candidates: usize,
        pause_release: std::time::Duration,
        rearm: std::time::Duration,
        progress_bucket: std::time::Duration,
    ) -> Result<Self, String> {
        if input_bytes_per_stream == 0 || max_candidates == 0 {
            return Err(
                "scan preparation input bytes and candidate count must be nonzero".to_string(),
            );
        }
        if pause_release.is_zero() || rearm.is_zero() || progress_bucket.is_zero() {
            return Err("scan preparation durations must be nonzero".to_string());
        }
        if !rearm.as_nanos().is_multiple_of(progress_bucket.as_nanos()) {
            return Err(
                "scan preparation rearm must be a multiple of its progress bucket".to_string(),
            );
        }
        Ok(Self {
            input_bytes_per_stream,
            max_candidates,
            pause_release,
            rearm,
            progress_bucket,
        })
    }
}

impl Default for ScanPreparationConfig {
    fn default() -> Self {
        Self::try_new(
            64 * 1024 * 1024,
            4,
            std::time::Duration::from_millis(500),
            std::time::Duration::from_millis(500),
            std::time::Duration::from_millis(100),
        )
        .expect("valid default scan preparation configuration")
    }
}

impl TypedScanRuntime {
    pub fn new(
        execution_id: QueryExecutionId,
        catalog_read_execution: CatalogReadExecutionResolver,
        catalog_write_execution: CatalogWriteExecutionResolver,
        queues: Arc<SplitQueues<ReceivedReadSplit>>,
        session: ConnectorSession,
        runtime_filter: RuntimeFilterSessionResolver,
        read_context: Arc<TaskReadBindingRegistry>,
        storage_resolver: Arc<dyn ConnectorStorageResolver>,
        stream_host: ScanStreamHost,
    ) -> Self {
        let connector_resource_ledger = Arc::new(WorkerConnectorResourceLedger::new());
        Self {
            execution_id,
            catalog_read_execution,
            catalog_write_execution,
            queues,
            session,
            runtime_filter,
            read_context,
            storage_resolver,
            connector_resource_ledger,
            stream_host,
        }
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub fn stream_host(&self) -> &ScanStreamHost {
        &self.stream_host
    }

    pub fn catalog_read_execution(
        &self,
        handle: &CatalogHandle,
    ) -> Result<ConnectorExecutionReadBinding, CatalogExecutionAccessError> {
        (self.catalog_read_execution)(handle)
    }

    pub fn catalog_write_execution(
        &self,
        handle: &CatalogHandle,
    ) -> Result<ConnectorExecutionWriteBinding, CatalogExecutionAccessError> {
        (self.catalog_write_execution)(handle)
    }

    pub fn queues(&self) -> Arc<SplitQueues<ReceivedReadSplit>> {
        Arc::clone(&self.queues)
    }

    pub fn session(&self) -> ConnectorSession {
        self.session.clone()
    }

    pub fn runtime_filter(&self) -> RuntimeFilterSessionResolver {
        Arc::clone(&self.runtime_filter)
    }

    pub fn storage_resolver(&self) -> Arc<dyn ConnectorStorageResolver> {
        Arc::clone(&self.storage_resolver)
    }

    /// Issue execution resources only for this exact admitted attempt and
    /// tracker. Decode may retain `TypedScanRuntime`, but cannot obtain this
    /// capability before the task host installs its real fragment tracker.
    pub fn admitted_connector_resources(
        &self,
        execution_id: QueryExecutionId,
        tracker: &Arc<MemTracker>,
    ) -> Result<ConnectorExecutionResources, ConnectorResourceBindingError> {
        if execution_id != self.execution_id {
            return Err(ConnectorResourceBindingError::WrongExecution);
        }
        self.connector_resource_ledger.admitted_resources(tracker)
    }

    /// Resolve the tracker installed by this task's admission. Decode holds
    /// this runtime before admission, but provider binding can obtain a real
    /// resource capability only after the host installs that tracker.
    pub fn admitted_connector_resources_for_bound_task(
        &self,
        execution_id: QueryExecutionId,
    ) -> Result<ConnectorExecutionResources, ConnectorResourceBindingError> {
        let tracker = self
            .connector_resource_ledger
            .tracker
            .get()
            .ok_or(ConnectorResourceBindingError::BeforeAdmission)?;
        self.admitted_connector_resources(execution_id, tracker)
    }

    pub fn install_connector_resource_tracker(
        &self,
        tracker: Arc<MemTracker>,
    ) -> Result<(), ConnectorResourceBindingError> {
        self.connector_resource_ledger.install(tracker)
    }

    pub fn register_read_execution(
        &self,
        plan_node_id: i32,
        execution: ConnectorExecutionReadBinding,
    ) -> Result<(), crate::read_attempt::ReadExecutionRegistrationError> {
        self.read_context.register(plan_node_id, execution)
    }
}

#[cfg(test)]
mod tests {
    use super::WorkerConnectorResourceLedger;
    use novarocks_execution::runtime::mem_tracker::MemTracker;
    use novarocks_spi::connector::{
        ConnectorErrorKind, ConnectorResourceClass, ConnectorResourceLedger,
    };

    #[test]
    fn connector_ledger_rejects_reservation_before_fragment_admission() {
        let ledger = WorkerConnectorResourceLedger::new();

        let error = ledger
            .try_reserve(ConnectorResourceClass::ReaderState, 1)
            .err()
            .expect("reservation before tracker installation must fail closed");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(
            error
                .to_string()
                .contains("before native fragment admission")
        );
    }

    #[test]
    fn connector_lease_charges_and_releases_the_exact_tracker_chain() {
        let process = MemTracker::new_root("process");
        let query = MemTracker::new_child("query", &process);
        let fragment = MemTracker::new_child("fragment", &query);
        let ledger = WorkerConnectorResourceLedger::new();
        ledger
            .install(fragment.clone())
            .expect("fragment tracker installation succeeds");

        let mut lease = ledger
            .try_reserve(ConnectorResourceClass::ReaderOutput, 8)
            .expect("reservation charges installed tracker");
        assert_eq!(lease.bytes(), 8);
        assert_eq!(fragment.current(), 8);
        assert_eq!(query.current(), 8);
        assert_eq!(process.current(), 8);

        lease.try_grow(5).expect("lease growth is charged");
        assert_eq!(fragment.current(), 13);
        assert_eq!(query.current(), 13);
        assert_eq!(process.current(), 13);

        lease.shrink_to(3).expect("lease shrink is released");
        assert_eq!(fragment.current(), 3);
        assert_eq!(query.current(), 3);
        assert_eq!(process.current(), 3);

        drop(lease);
        assert_eq!(fragment.current(), 0);
        assert_eq!(query.current(), 0);
        assert_eq!(process.current(), 0);
        assert_eq!(fragment.allocated(), 13);
        assert_eq!(fragment.deallocated(), 13);
    }

    #[test]
    fn connector_ledger_only_allows_idempotent_tracker_installation() {
        let ledger = WorkerConnectorResourceLedger::new();
        let admitted_fragment = MemTracker::new_root("admitted-fragment");
        let other_fragment = MemTracker::new_root("other-fragment");

        ledger
            .install(admitted_fragment.clone())
            .expect("first tracker installation succeeds");
        ledger
            .install(admitted_fragment)
            .expect("exact tracker replay is idempotent");

        let error = ledger
            .install(other_fragment)
            .expect_err("rebinding to another fragment must fail closed");
        assert_eq!(error, super::ConnectorResourceBindingError::TrackerRebound);
    }

    #[test]
    fn execution_resources_require_the_installed_fragment_and_charge_it() {
        let ledger = std::sync::Arc::new(WorkerConnectorResourceLedger::new());
        let admitted = MemTracker::new_root("admitted-fragment");
        let foreign = MemTracker::new_root("foreign-fragment");
        assert!(ledger.admitted_resources(&admitted).is_err());
        ledger
            .install(admitted.clone())
            .expect("install the admitted fragment");
        assert!(ledger.admitted_resources(&foreign).is_err());
        let resources = ledger
            .admitted_resources(&admitted)
            .expect("obtain the exact fragment's resources");
        let charge = resources
            .try_reserve(ConnectorResourceClass::ReaderState, 11)
            .expect("charge the real tracker");
        assert_eq!(admitted.current(), 11);
        assert_eq!(foreign.current(), 0);
        drop(charge);
        assert_eq!(admitted.current(), 0);

        admitted.install_limit_once(10).expect("set admitted limit");
        let error = resources
            .try_reserve(ConnectorResourceClass::ReaderState, 11)
            .expect_err("reservation above the real tracker limit must fail");
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
        assert_eq!(admitted.current(), 0);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorResourceBindingError {
    TrackerRebound,
    InstallationRaced,
    BeforeAdmission,
    WrongTracker,
    WrongExecution,
}
impl std::fmt::Display for ConnectorResourceBindingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::TrackerRebound => {
                "connector resource ledger was rebound to another fragment tracker"
            }
            Self::InstallationRaced => "connector resource ledger installation raced",
            Self::BeforeAdmission => "connector resources requested before task admission",
            Self::WrongTracker => "connector resource request has the wrong fragment tracker",
            Self::WrongExecution => "connector resource request belongs to another execution",
        })
    }
}
impl std::error::Error for ConnectorResourceBindingError {}
impl novarocks_execution_contract::failure::FailureProjection for ConnectorResourceBindingError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use crate::failure_reason::WorkerFailureReason as R;
        use novarocks_execution_contract::failure::{FailureClass, FailureFact};
        let reason = match self {
            Self::TrackerRebound => R::ConnectorTrackerRebound,
            Self::InstallationRaced => R::ConnectorTrackerInstallRaced,
            Self::BeforeAdmission => R::ConnectorBeforeAdmission,
            Self::WrongTracker => R::ConnectorWrongTracker,
            Self::WrongExecution => R::ConnectorWrongExecution,
        };
        FailureFact::new(FailureClass::Invariant, reason.reason())
    }
}
impl From<ConnectorResourceBindingError> for novarocks_execution::runtime::task::ExecutionFailure {
    fn from(cause: ConnectorResourceBindingError) -> Self {
        Self::from_domain(cause)
    }
}
