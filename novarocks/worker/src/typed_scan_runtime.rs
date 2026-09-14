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

use novarocks_execution::connector::TaskAttemptSplitQueues;
use novarocks_execution::runtime::mem_tracker::MemTracker;
use novarocks_execution::runtime_filter::RuntimeFilterSessionRef;
use novarocks_spi::connector::read_stack::ConnectorSession;
use novarocks_spi::connector::{
    CatalogHandle, ConnectorError, ConnectorErrorKind, ConnectorExecutionReadBinding,
    ConnectorExecutionWriteBinding, ConnectorRequestResources, ConnectorResourceCheckpoint,
    ConnectorResourceClass, ConnectorResourceLease, ConnectorResourceLedger,
    ConnectorStorageResolver,
};
use novarocks_types::QueryExecutionId;

use crate::read_attempt::{ReceivedReadSplit, TypedReadAttemptContext};

/// Looks up an attempt's runtime-filter session at the moment a scan binds it.
pub type RuntimeFilterSessionResolver =
    Arc<dyn Fn() -> Result<Option<RuntimeFilterSessionRef>, String> + Send + Sync>;

/// Resolves one reader only through an attempt's query-leased immutable catalog
/// runtime. It intentionally cannot reinstall a provider runtime.
pub type CatalogReadExecutionResolver =
    Arc<dyn Fn(&CatalogHandle) -> Result<ConnectorExecutionReadBinding, String> + Send + Sync>;

/// Resolves one writer only through an attempt's query-leased immutable catalog
/// runtime.
pub type CatalogWriteExecutionResolver =
    Arc<dyn Fn(&CatalogHandle) -> Result<ConnectorExecutionWriteBinding, String> + Send + Sync>;

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

    fn install(&self, tracker: Arc<MemTracker>) -> Result<(), String> {
        if let Some(existing) = self.tracker.get() {
            return if Arc::ptr_eq(existing, &tracker) {
                Ok(())
            } else {
                Err("connector resource ledger was rebound to another fragment tracker".to_string())
            };
        }
        self.tracker
            .set(tracker)
            .map_err(|_| "connector resource ledger installation raced".to_string())
    }
}

struct WorkerConnectorResourceLease {
    tracker: Arc<MemTracker>,
    bytes: i64,
}

impl ConnectorResourceLease for WorkerConnectorResourceLease {
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

impl Drop for WorkerConnectorResourceLease {
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
    ) -> Result<Box<dyn ConnectorResourceLease>, ConnectorError> {
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
        Ok(Box::new(WorkerConnectorResourceLease { tracker, bytes }))
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
    queues: Arc<TaskAttemptSplitQueues<ReceivedReadSplit>>,
    session: ConnectorSession,
    runtime_filter: RuntimeFilterSessionResolver,
    read_context: Arc<TypedReadAttemptContext>,
    storage_resolver: Arc<dyn ConnectorStorageResolver>,
    connector_resources: ConnectorRequestResources,
    connector_resource_ledger: Arc<WorkerConnectorResourceLedger>,
}

impl TypedScanRuntime {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        execution_id: QueryExecutionId,
        catalog_read_execution: CatalogReadExecutionResolver,
        catalog_write_execution: CatalogWriteExecutionResolver,
        queues: Arc<TaskAttemptSplitQueues<ReceivedReadSplit>>,
        session: ConnectorSession,
        runtime_filter: RuntimeFilterSessionResolver,
        read_context: Arc<TypedReadAttemptContext>,
        storage_resolver: Arc<dyn ConnectorStorageResolver>,
    ) -> Self {
        let connector_resource_ledger = Arc::new(WorkerConnectorResourceLedger::new());
        let connector_resources = ConnectorRequestResources::new(Arc::clone(
            &connector_resource_ledger,
        )
            as Arc<dyn ConnectorResourceLedger>);
        Self {
            execution_id,
            catalog_read_execution,
            catalog_write_execution,
            queues,
            session,
            runtime_filter,
            read_context,
            storage_resolver,
            connector_resources,
            connector_resource_ledger,
        }
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub fn catalog_read_execution(
        &self,
        handle: &CatalogHandle,
    ) -> Result<ConnectorExecutionReadBinding, String> {
        (self.catalog_read_execution)(handle)
    }

    pub fn catalog_write_execution(
        &self,
        handle: &CatalogHandle,
    ) -> Result<ConnectorExecutionWriteBinding, String> {
        (self.catalog_write_execution)(handle)
    }

    pub fn queues(&self) -> Arc<TaskAttemptSplitQueues<ReceivedReadSplit>> {
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

    pub fn connector_resources(&self) -> ConnectorRequestResources {
        self.connector_resources.clone()
    }

    pub fn install_connector_resource_tracker(
        &self,
        tracker: Arc<MemTracker>,
    ) -> Result<(), String> {
        self.connector_resource_ledger.install(tracker)
    }

    pub fn register_read_execution(
        &self,
        plan_node_id: i32,
        execution: ConnectorExecutionReadBinding,
    ) -> Result<(), String> {
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
        assert!(error.contains("rebound to another fragment tracker"));
    }
}
