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
use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use crate::runtime::descriptor_snapshot::DescriptorSnapshot;
use novarocks_execution::exec::node::scan::IncrementalScanRange;
use novarocks_execution::exec::node::scan::ScanOp;
use novarocks_execution::exec::operators::scan::dispatch::ScanDispatchState;
use novarocks_execution::runtime::fragment::io::ExchangeReceiverPort;
use novarocks_execution::runtime::mem_tracker::{self, MemTracker};
use novarocks_types::SlotId;
use novarocks_types::UniqueId;

pub(crate) use novarocks_types::QueryId;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum QueryExecutionGeneration {
    Native(NonZeroU64),
}

#[allow(
    dead_code,
    reason = "Legacy query-context callers retain the first native attempt outside the lifecycle-aware backend target."
)]
fn legacy_native_attempt() -> NonZeroU64 {
    NonZeroU64::new(1).expect("one is a nonzero native attempt")
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct QueryExecutionKey {
    query_id: QueryId,
    generation: QueryExecutionGeneration,
}

impl QueryExecutionKey {
    /// Legacy native callers predate lifecycle attempts. New native lifecycle
    /// code must use `native_attempt`.
    #[allow(
        dead_code,
        reason = "Legacy query-context callers retain this non-lifecycle execution key constructor."
    )]
    pub(crate) fn native(query_id: QueryId) -> Self {
        Self {
            query_id,
            generation: QueryExecutionGeneration::Native(legacy_native_attempt()),
        }
    }

    pub(crate) const fn native_attempt(query_id: QueryId, attempt: NonZeroU64) -> Self {
        Self {
            query_id,
            generation: QueryExecutionGeneration::Native(attempt),
        }
    }

    pub(crate) const fn query_id(self) -> QueryId {
        self.query_id
    }

    pub(crate) const fn native_attempt_id(self) -> Option<NonZeroU64> {
        match self.generation {
            QueryExecutionGeneration::Native(attempt) => Some(attempt),
        }
    }
}

#[allow(
    dead_code,
    reason = "Cleanup leases are retained for execution integrations that own query-scoped external resources."
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueryContextGeneration {
    Native(NonZeroU64),
}

pub struct QueryCleanupLease {
    release: Option<Box<dyn FnOnce() + Send + 'static>>,
}

#[allow(
    dead_code,
    reason = "Cleanup lease methods are retained for execution integrations that own query-scoped external resources."
)]
impl QueryCleanupLease {
    /// Creates a query-scoped cleanup action for a consumer-owned resource.
    pub fn from_release(release: impl FnOnce() + Send + 'static) -> Self {
        Self {
            release: Some(Box::new(release)),
        }
    }

    pub(crate) fn new(release: impl FnOnce() + Send + 'static) -> Self {
        Self::from_release(release)
    }

    pub(crate) fn release(mut self) {
        if let Some(release) = self.release.take() {
            release();
        }
    }
}

impl Drop for QueryCleanupLease {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            release();
        }
    }
}

#[cfg(test)]
mod query_cleanup_lease_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::QueryCleanupLease;

    #[test]
    fn consumer_owned_release_runs_once_when_lease_drops() {
        let releases = Arc::new(AtomicUsize::new(0));
        let release_counter = Arc::clone(&releases);
        let lease = QueryCleanupLease::from_release(move || {
            release_counter.fetch_add(1, Ordering::SeqCst);
        });
        drop(lease);
        assert_eq!(releases.load(Ordering::SeqCst), 1);
    }
}

#[allow(
    dead_code,
    reason = "Query context fields are consumed by native execution integrations outside the backend lib test configuration."
)]
pub(crate) struct QueryContext {
    #[allow(dead_code)]
    pub(crate) query_id: QueryId,
    execution_generation: QueryContextGeneration,
    pub(crate) desc_snapshot: Option<Arc<DescriptorSnapshot>>,
    pub(crate) num_fragments: usize,
    pub(crate) num_active_fragments: usize,
    pub(crate) total_fragments: Option<usize>,
    pub(crate) cancelled_by_fe: bool,
    pub(crate) delivery_expire: Duration,
    pub(crate) delivery_deadline: Instant,
    #[allow(dead_code)]
    pub(crate) query_expire: Duration,
    #[allow(dead_code)]
    pub(crate) query_deadline: Instant,
    pub(crate) mem_tracker: Arc<MemTracker>,
    cleanup_leases: Vec<QueryCleanupLease>,
}

#[derive(Default)]
struct RuntimeFilterQueryCancellationAction;

#[allow(
    dead_code,
    reason = "Query context helpers are consumed by native execution integrations outside the backend lib test configuration."
)]
impl QueryContext {
    pub(crate) fn new(
        query_id: QueryId,
        delivery_expire: Duration,
        query_expire: Duration,
    ) -> Self {
        Self::new_with_generation(
            query_id,
            QueryContextGeneration::Native(legacy_native_attempt()),
            delivery_expire,
            query_expire,
        )
    }

    fn new_with_generation(
        query_id: QueryId,
        execution_generation: QueryContextGeneration,
        delivery_expire: Duration,
        query_expire: Duration,
    ) -> Self {
        let now = Instant::now();
        let process = mem_tracker::process_mem_tracker();
        let query_label = format!("query_{:x}_{:x}", query_id.high(), query_id.low());
        let mem_tracker = MemTracker::new_child(query_label, &process);
        Self {
            query_id,
            execution_generation,
            desc_snapshot: None,
            num_fragments: 0,
            num_active_fragments: 0,
            total_fragments: None,
            cancelled_by_fe: false,
            delivery_expire,
            delivery_deadline: now + delivery_expire,
            query_expire,
            query_deadline: now + query_expire,
            mem_tracker,
            cleanup_leases: Vec::new(),
        }
    }

    fn matches_execution(&self, key: QueryExecutionKey) -> bool {
        self.query_id == key.query_id
            && match (self.execution_generation, key.generation) {
                (
                    QueryContextGeneration::Native(current),
                    QueryExecutionGeneration::Native(requested),
                ) => current == requested,
            }
    }

    pub(crate) fn increment_num_fragments(&mut self) {
        self.num_fragments += 1;
        self.num_active_fragments += 1;
    }

    pub(crate) fn attach_cleanup_lease(&mut self, lease: QueryCleanupLease) {
        self.cleanup_leases.push(lease);
    }

    #[allow(dead_code)]
    pub(crate) fn rollback_inc_fragments(&mut self) {
        self.num_fragments = self.num_fragments.saturating_sub(1);
        self.num_active_fragments = self.num_active_fragments.saturating_sub(1);
    }

    pub(crate) fn count_down_fragments(&mut self) -> bool {
        if self.num_active_fragments > 0 {
            self.num_active_fragments -= 1;
        }
        self.num_active_fragments == 0
    }

    pub(crate) fn has_no_active_instances(&self) -> bool {
        self.num_active_fragments == 0
    }

    pub(crate) fn is_dead(&self) -> bool {
        self.num_active_fragments == 0
            && (self.cancelled_by_fe
                || self
                    .total_fragments
                    .map(|t| self.num_fragments >= t)
                    .unwrap_or(false))
    }

    pub(crate) fn is_delivery_expired(&self) -> bool {
        Instant::now() >= self.delivery_deadline
    }

    pub(crate) fn is_query_expired(&self) -> bool {
        Instant::now() >= self.query_deadline
    }

    pub(crate) fn extend_delivery_lifetime(&mut self) {
        self.delivery_deadline = Instant::now() + self.delivery_expire;
    }

    pub(crate) fn mem_tracker(&self) -> Arc<MemTracker> {
        Arc::clone(&self.mem_tracker)
    }
}

struct IncrementalScanNodeHandle {
    op: Arc<dyn ScanOp>,
    dispatch: Arc<ScanDispatchState>,
    update_mu: Mutex<()>,
}

impl IncrementalScanNodeHandle {
    fn new(op: Arc<dyn ScanOp>, dispatch: Arc<ScanDispatchState>) -> Self {
        Self {
            op,
            dispatch,
            update_mu: Mutex::new(()),
        }
    }

    fn append_scan_ranges(&self, scan_ranges: &[IncrementalScanRange]) -> Result<(), String> {
        let _guard = self.update_mu.lock().expect("incremental scan handle lock");
        let morsels = self.op.build_incremental_morsels(scan_ranges)?;
        self.dispatch
            .append_morsels(morsels.morsels, morsels.has_more)
    }
}

#[derive(Default)]
struct QueryContextManagerInner {
    active: HashMap<QueryId, QueryContext>,
    second_chance: HashMap<QueryId, QueryContext>,
    finst_to_query: HashMap<UniqueId, QueryExecutionKey>,
    exchange_receiver_ports: HashMap<UniqueId, Arc<dyn ExchangeReceiverPort>>,
    incremental_scan_nodes: HashMap<UniqueId, HashMap<i32, Arc<IncrementalScanNodeHandle>>>,
    pending_incremental_scan_ranges: HashMap<UniqueId, HashMap<i32, Vec<IncrementalScanRange>>>,
    incremental_change_op_slots: HashMap<UniqueId, HashMap<i32, Option<SlotId>>>,
}

pub(crate) struct QueryContextManager {
    inner: Mutex<QueryContextManagerInner>,
    stopped: AtomicBool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NativeQueryExecutionResourceSnapshot {
    pub active_contexts: usize,
    pub second_chance_contexts: usize,
    pub active_fragments: usize,
}

#[allow(
    dead_code,
    reason = "Fragment cancellation results are retained for native execution integrations outside the backend lib test configuration."
)]
pub(crate) struct FinstCancelResult {
    pub(crate) query_id: Option<QueryId>,
    pub(crate) finsts: Vec<UniqueId>,
}

#[allow(
    dead_code,
    reason = "Query-context manager APIs are consumed by native execution integrations outside the backend lib test configuration."
)]
impl QueryContextManager {
    pub fn native_execution_resource_snapshot(&self) -> NativeQueryExecutionResourceSnapshot {
        let inner = self.inner.lock().expect("query context manager lock");
        let mut snapshot = NativeQueryExecutionResourceSnapshot::default();
        for context in inner.active.values() {
            if matches!(
                context.execution_generation,
                QueryContextGeneration::Native(_)
            ) {
                snapshot.active_contexts += 1;
                snapshot.active_fragments += context.num_active_fragments;
            }
        }
        for context in inner.second_chance.values() {
            if matches!(
                context.execution_generation,
                QueryContextGeneration::Native(_)
            ) {
                snapshot.second_chance_contexts += 1;
                snapshot.active_fragments += context.num_active_fragments;
            }
        }
        snapshot
    }
    fn new() -> Arc<Self> {
        let manager = Arc::new(Self {
            inner: Mutex::new(QueryContextManagerInner::default()),
            stopped: AtomicBool::new(false),
        });
        let mgr = Arc::clone(&manager);
        thread::spawn(move || mgr.clean_loop());
        manager
    }

    #[cfg(test)]
    pub(crate) fn new_for_test() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(QueryContextManagerInner::default()),
            stopped: AtomicBool::new(false),
        })
    }

    #[cfg(test)]
    pub(crate) fn new_for_live_test() -> (Arc<Self>, thread::JoinHandle<()>) {
        Self::new_for_live_test_with_exit_signal(None)
    }

    #[cfg(test)]
    pub(crate) fn new_for_live_test_with_exit_signal(
        exited: Option<std::sync::mpsc::SyncSender<()>>,
    ) -> (Arc<Self>, thread::JoinHandle<()>) {
        let manager = Arc::new(Self {
            inner: Mutex::new(QueryContextManagerInner::default()),
            stopped: AtomicBool::new(false),
        });
        let clean_manager = Arc::clone(&manager);
        let clean_handle = thread::spawn(move || {
            clean_manager.clean_loop();
            if let Some(exited) = exited {
                let _ = exited.send(());
            }
        });
        (manager, clean_handle)
    }

    #[cfg(test)]
    pub(crate) fn stop_clean_loop_for_test(&self) {
        self.stopped.store(true, Ordering::Relaxed);
    }

    fn clean_loop(self: Arc<Self>) {
        while !self.stopped.load(Ordering::Relaxed) {
            self.clean_expired();
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn clean_expired(&self) {
        let expired = {
            let mut guard = self.inner.lock().expect("query_ctx_manager lock");
            let expired_second_chance = guard
                .second_chance
                .iter()
                .filter_map(|(qid, ctx)| {
                    (ctx.has_no_active_instances() && ctx.is_delivery_expired()).then_some(*qid)
                })
                .collect::<Vec<_>>();
            let expired_active = guard
                .active
                .iter()
                .filter_map(|(qid, ctx)| {
                    (ctx.has_no_active_instances() && ctx.is_query_expired()).then_some(*qid)
                })
                .collect::<Vec<_>>();
            let mut expired = Vec::with_capacity(
                expired_second_chance
                    .len()
                    .saturating_add(expired_active.len()),
            );
            expired.extend(
                expired_second_chance
                    .into_iter()
                    .filter_map(|qid| guard.second_chance.remove(&qid).map(|ctx| (qid, ctx))),
            );
            expired.extend(
                expired_active
                    .into_iter()
                    .filter_map(|qid| guard.active.remove(&qid).map(|ctx| (qid, ctx))),
            );
            expired
        };
        drop(expired);
    }

    #[cfg(test)]
    pub(crate) fn clean_expired_for_test(&self) {
        self.clean_expired();
    }

    #[cfg(test)]
    pub(crate) fn expire_delivery_for_test(&self, query_id: QueryId) {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        let context = if guard.active.contains_key(&query_id) {
            guard.active.get_mut(&query_id).expect("checked active")
        } else {
            guard
                .second_chance
                .get_mut(&query_id)
                .expect("query context must exist")
        };
        context.delivery_deadline = Instant::now() - Duration::from_millis(1);
    }

    #[cfg(test)]
    pub(crate) fn fragment_counts_for_test(&self, query_id: QueryId) -> Option<(usize, usize)> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .map(|context| (context.num_fragments, context.num_active_fragments))
    }

    fn get_or_register(
        &self,
        query_id: QueryId,
        return_error_if_not_exist: bool,
        delivery_expire: Duration,
        query_expire: Duration,
    ) -> Result<(), String> {
        self.get_or_register_internal(
            query_id,
            return_error_if_not_exist,
            delivery_expire,
            query_expire,
            true,
        )
    }

    pub(crate) fn get_or_register_native(
        &self,
        query_id: QueryId,
        return_error_if_not_exist: bool,
        delivery_expire: Duration,
        query_expire: Duration,
    ) -> Result<(), String> {
        self.get_or_register_internal_with_generation(
            query_id,
            return_error_if_not_exist,
            delivery_expire,
            query_expire,
            true,
            QueryContextGeneration::Native(legacy_native_attempt()),
            true,
        )
    }

    pub(crate) fn ensure_native_context(
        &self,
        query_id: QueryId,
        return_error_if_not_exist: bool,
        delivery_expire: Duration,
        query_expire: Duration,
    ) -> Result<(), String> {
        self.get_or_register_internal_with_generation(
            query_id,
            return_error_if_not_exist,
            delivery_expire,
            query_expire,
            false,
            QueryContextGeneration::Native(legacy_native_attempt()),
            true,
        )
    }

    pub(crate) fn ensure_native_context_execution(
        &self,
        execution: QueryExecutionKey,
        return_error_if_not_exist: bool,
        delivery_expire: Duration,
        query_expire: Duration,
    ) -> Result<(), String> {
        let Some(attempt) = execution.native_attempt_id() else {
            return Err("native context requires a native execution key".to_string());
        };
        self.get_or_register_internal_with_generation(
            execution.query_id(),
            return_error_if_not_exist,
            delivery_expire,
            query_expire,
            false,
            QueryContextGeneration::Native(attempt),
            true,
        )
    }

    pub(crate) fn get_or_register_native_execution(
        &self,
        execution: QueryExecutionKey,
        return_error_if_not_exist: bool,
        delivery_expire: Duration,
        query_expire: Duration,
    ) -> Result<(), String> {
        let Some(attempt) = execution.native_attempt_id() else {
            return Err("native context requires a native execution key".to_string());
        };
        self.get_or_register_internal_with_generation(
            execution.query_id(),
            return_error_if_not_exist,
            delivery_expire,
            query_expire,
            true,
            QueryContextGeneration::Native(attempt),
            true,
        )
    }

    fn ensure_context(
        &self,
        query_id: QueryId,
        return_error_if_not_exist: bool,
        delivery_expire: Duration,
        query_expire: Duration,
    ) -> Result<(), String> {
        self.get_or_register_internal(
            query_id,
            return_error_if_not_exist,
            delivery_expire,
            query_expire,
            false,
        )
    }

    fn get_or_register_internal(
        &self,
        query_id: QueryId,
        return_error_if_not_exist: bool,
        delivery_expire: Duration,
        query_expire: Duration,
        increment: bool,
    ) -> Result<(), String> {
        self.get_or_register_internal_with_generation(
            query_id,
            return_error_if_not_exist,
            delivery_expire,
            query_expire,
            increment,
            QueryContextGeneration::Native(legacy_native_attempt()),
            false,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "This internal constructor accepts the complete query registration contract before atomically installing context state."
    )]
    fn get_or_register_internal_with_generation(
        &self,
        query_id: QueryId,
        return_error_if_not_exist: bool,
        delivery_expire: Duration,
        query_expire: Duration,
        increment: bool,
        generation: QueryContextGeneration,
        _native_runtime_filter_lifecycle: bool,
    ) -> Result<(), String> {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        if let Some(ctx) = guard.active.get_mut(&query_id) {
            if increment {
                ctx.increment_num_fragments();
            }
            return Ok(());
        }
        if guard.second_chance.contains_key(&query_id) {
            let mut ctx = guard.second_chance.remove(&query_id).expect("checked");
            if increment {
                ctx.increment_num_fragments();
            }
            guard.active.insert(query_id, ctx);
            return Ok(());
        }
        if return_error_if_not_exist {
            return Err("Query terminates prematurely (missing QueryContext)".to_string());
        }
        let mut ctx =
            QueryContext::new_with_generation(query_id, generation, delivery_expire, query_expire);
        if increment {
            ctx.increment_num_fragments();
        }
        guard.active.insert(query_id, ctx);
        Ok(())
    }

    pub(crate) fn with_context_mut<T, F>(&self, query_id: QueryId, f: F) -> Result<T, String>
    where
        F: FnOnce(&mut QueryContext) -> Result<T, String>,
    {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        let ctx = guard
            .active
            .get_mut(&query_id)
            .ok_or_else(|| "QueryContext not found".to_string())?;
        f(ctx)
    }

    pub(crate) fn attach_cleanup_lease(
        &self,
        query_id: QueryId,
        lease: QueryCleanupLease,
    ) -> Result<(), String> {
        self.with_context_mut(query_id, |ctx| {
            ctx.attach_cleanup_lease(lease);
            Ok(())
        })
    }

    #[cfg(test)]
    pub(crate) fn query_ids_for_test(&self) -> Vec<QueryId> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        let mut query_ids = guard
            .active
            .keys()
            .chain(guard.second_chance.keys())
            .copied()
            .collect::<Vec<_>>();
        query_ids.sort_by_key(|query_id| (query_id.high(), query_id.low()));
        query_ids.dedup();
        query_ids
    }

    /// Returns the query tracker for lifecycle verification and neutral runtime observers.
    pub fn query_mem_tracker(&self, query_id: QueryId) -> Option<Arc<MemTracker>> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .map(|ctx| ctx.mem_tracker())
    }

    pub(crate) fn query_mem_tracker_execution(
        &self,
        execution: QueryExecutionKey,
    ) -> Option<Arc<MemTracker>> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&execution.query_id())
            .or_else(|| guard.second_chance.get(&execution.query_id()))
            .filter(|context| context.matches_execution(execution))
            .map(QueryContext::mem_tracker)
    }

    pub(crate) fn descriptor_snapshot(&self, query_id: QueryId) -> Option<Arc<DescriptorSnapshot>> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .and_then(|ctx| ctx.desc_snapshot.clone())
    }

    pub(crate) fn register_incremental_scan_node(
        &self,
        finst_id: UniqueId,
        node_id: i32,
        op: Arc<dyn ScanOp>,
        dispatch: Arc<ScanDispatchState>,
    ) -> Result<(), String> {
        let handle = {
            let mut guard = self.inner.lock().expect("query_ctx_manager lock");
            if !guard.finst_to_query.contains_key(&finst_id) {
                return Ok(());
            }
            let node_map = guard.incremental_scan_nodes.entry(finst_id).or_default();
            if let Some(existing) = node_map.get(&node_id) {
                Arc::clone(existing)
            } else {
                let handle = Arc::new(IncrementalScanNodeHandle::new(op, dispatch));
                node_map.insert(node_id, Arc::clone(&handle));
                handle
            }
        };

        let pending = {
            let mut guard = self.inner.lock().expect("query_ctx_manager lock");
            guard
                .pending_incremental_scan_ranges
                .get_mut(&finst_id)
                .and_then(|node_map| node_map.remove(&node_id))
        };
        if let Some(scan_ranges) = pending {
            handle.append_scan_ranges(&scan_ranges)?;
        }
        Ok(())
    }

    pub(crate) fn append_incremental_scan_ranges(
        &self,
        finst_id: UniqueId,
        node_id: i32,
        mut scan_ranges: Vec<IncrementalScanRange>,
    ) -> Result<(), String> {
        if scan_ranges.is_empty() {
            return Ok(());
        }
        let handle = {
            let mut guard = self.inner.lock().expect("query_ctx_manager lock");
            if let Some(handle) = guard
                .incremental_scan_nodes
                .get(&finst_id)
                .and_then(|node_map| node_map.get(&node_id))
            {
                Some(Arc::clone(handle))
            } else if guard.finst_to_query.contains_key(&finst_id) {
                guard
                    .pending_incremental_scan_ranges
                    .entry(finst_id)
                    .or_default()
                    .entry(node_id)
                    .or_default()
                    .append(&mut scan_ranges);
                None
            } else {
                None
            }
        };
        if let Some(handle) = handle {
            handle.append_scan_ranges(&scan_ranges)?;
        }
        Ok(())
    }

    pub(crate) fn incremental_change_op_slot(
        &self,
        finst_id: UniqueId,
        node_id: i32,
    ) -> Result<Option<SlotId>, String> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .incremental_change_op_slots
            .get(&finst_id)
            .and_then(|contracts| contracts.get(&node_id))
            .copied()
            .ok_or_else(|| {
                format!(
                    "incremental scan range has no registered scan contract for finst_id={finst_id} node_id={node_id}"
                )
            })
    }

    #[cfg(test)]
    fn pending_incremental_scan_ranges_for_test(
        &self,
        finst_id: UniqueId,
        node_id: i32,
    ) -> Vec<IncrementalScanRange> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .pending_incremental_scan_ranges
            .get(&finst_id)
            .and_then(|nodes| nodes.get(&node_id))
            .cloned()
            .unwrap_or_default()
    }

    pub(crate) fn register_finst(&self, finst_id: UniqueId, query_id: QueryId) {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .finst_to_query
            .insert(finst_id, QueryExecutionKey::native(query_id));
    }

    /// Associates a fragment with the application-owned exchange capability
    /// used by ingress and operators. Cancellation snapshots this capability
    /// under the manager lock, then invokes it only after releasing the lock.
    pub(crate) fn register_exchange_receiver_port(
        &self,
        finst_id: UniqueId,
        port: Arc<dyn ExchangeReceiverPort>,
    ) {
        self.inner
            .lock()
            .expect("query context manager lock")
            .exchange_receiver_ports
            .insert(finst_id, port);
    }

    pub(crate) fn register_native_finst_execution(
        &self,
        finst_id: UniqueId,
        execution: QueryExecutionKey,
    ) -> Result<(), String> {
        if execution.native_attempt_id().is_none() {
            return Err("native finst registration requires a native execution key".to_string());
        }
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        let context = guard
            .active
            .get(&execution.query_id())
            .ok_or_else(|| "QueryContext not found".to_string())?;
        if !context.matches_execution(execution) {
            return Err("native finst registration belongs to another attempt".to_string());
        }
        guard.finst_to_query.insert(finst_id, execution);
        Ok(())
    }

    pub(crate) fn register_finsts<I>(&self, finst_ids: I, query_id: QueryId)
    where
        I: IntoIterator<Item = UniqueId>,
    {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        for finst_id in finst_ids {
            guard
                .finst_to_query
                .insert(finst_id, QueryExecutionKey::native(query_id));
        }
    }

    #[cfg(test)]
    pub(crate) fn register_finsts_with_incremental_contracts<I>(
        &self,
        instances: I,
        query_id: QueryId,
    ) where
        I: IntoIterator<Item = (UniqueId, HashMap<i32, Option<SlotId>>)>,
    {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        for (finst_id, contracts) in instances {
            guard
                .finst_to_query
                .insert(finst_id, QueryExecutionKey::native(query_id));
            guard
                .incremental_change_op_slots
                .insert(finst_id, contracts);
        }
    }

    /// Returns the owning query for a fragment instance when it is still registered.
    pub fn query_id_by_finst(&self, finst_id: UniqueId) -> Option<QueryId> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .finst_to_query
            .get(&finst_id)
            .map(|execution| execution.query_id())
    }

    pub(crate) fn query_execution_by_finst(&self, finst_id: UniqueId) -> Option<QueryExecutionKey> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard.finst_to_query.get(&finst_id).copied()
    }

    pub(crate) fn unregister_finst(&self, finst_id: UniqueId) {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        guard.finst_to_query.remove(&finst_id);
        guard.exchange_receiver_ports.remove(&finst_id);
        guard.incremental_scan_nodes.remove(&finst_id);
        guard.pending_incremental_scan_ranges.remove(&finst_id);
        guard.incremental_change_op_slots.remove(&finst_id);
    }

    /// Undo the registration performed before a native worker reports readiness.
    ///
    /// A synchronous pre-ready failure has not exposed a runnable fragment to the
    /// coordinator. Remove only that fragment's route and registration count. When
    /// it was the query's sole registration and no deployment owns the lifecycle,
    /// remove the otherwise empty query context as well.
    pub(crate) fn rollback_pre_ready_native_fragment(
        &self,
        query_id: QueryId,
        finst_id: UniqueId,
    ) -> bool {
        let removed = {
            let mut guard = self.inner.lock().expect("query_ctx_manager lock");
            if guard.finst_to_query.get(&finst_id) != Some(&QueryExecutionKey::native(query_id)) {
                return false;
            }
            let Some(context) = guard.active.get(&query_id) else {
                return false;
            };
            if !context.matches_execution(QueryExecutionKey::native(query_id))
                || context.num_fragments == 0
                || context.num_active_fragments == 0
            {
                return false;
            }

            guard.finst_to_query.remove(&finst_id);
            guard.exchange_receiver_ports.remove(&finst_id);
            let remove_empty_context = {
                let context = guard
                    .active
                    .get_mut(&query_id)
                    .expect("checked active context");
                context.rollback_inc_fragments();
                context.num_fragments == 0 && context.num_active_fragments == 0
            };
            remove_empty_context.then(|| {
                guard
                    .active
                    .remove(&query_id)
                    .expect("checked empty active context")
            })
        };
        drop(removed);
        true
    }

    pub(crate) fn rollback_pre_ready_native_fragment_execution(
        &self,
        execution: QueryExecutionKey,
        finst_id: UniqueId,
    ) -> bool {
        let query_id = execution.query_id();
        let removed = {
            let mut guard = self.inner.lock().expect("query_ctx_manager lock");
            if guard.finst_to_query.get(&finst_id) != Some(&execution) {
                return false;
            }
            let Some(context) = guard.active.get(&query_id) else {
                return false;
            };
            if !context.matches_execution(execution)
                || context.num_fragments == 0
                || context.num_active_fragments == 0
            {
                return false;
            }
            guard.finst_to_query.remove(&finst_id);
            let remove_empty_context = {
                let context = guard
                    .active
                    .get_mut(&query_id)
                    .expect("checked active context");
                context.rollback_inc_fragments();
                context.num_fragments == 0 && context.num_active_fragments == 0
            };
            remove_empty_context.then(|| {
                guard
                    .active
                    .remove(&query_id)
                    .expect("checked empty active context")
            })
        };
        drop(removed);
        true
    }

    pub(crate) fn unregister_finst_execution(
        &self,
        finst_id: UniqueId,
        execution: QueryExecutionKey,
    ) {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        if guard.finst_to_query.get(&finst_id) != Some(&execution) {
            return;
        }
        guard.finst_to_query.remove(&finst_id);
        guard.exchange_receiver_ports.remove(&finst_id);
        guard.incremental_scan_nodes.remove(&finst_id);
        guard.pending_incremental_scan_ranges.remove(&finst_id);
        guard.incremental_change_op_slots.remove(&finst_id);
    }

    pub(crate) fn get_query_timeout_by_finst(&self, finst_id: UniqueId) -> Option<Duration> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        let query_id = guard.finst_to_query.get(&finst_id)?.query_id();
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .map(|ctx| ctx.query_expire)
    }

    /// Read-only cancellation capability for protocol adapters that own scan planning.
    pub fn is_query_canceled(&self, query_id: QueryId) -> bool {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .map(|ctx| ctx.cancelled_by_fe)
            .or_else(|| {
                guard
                    .second_chance
                    .get(&query_id)
                    .map(|ctx| ctx.cancelled_by_fe)
            })
            .unwrap_or(false)
    }

    fn prepare_runtime_filter_query_cancellation(
        inner: &mut QueryContextManagerInner,
        query_id: QueryId,
        expected_execution: Option<QueryExecutionKey>,
        _cancellation_error: Option<&str>,
    ) -> RuntimeFilterQueryCancellationAction {
        let context = inner
            .active
            .get_mut(&query_id)
            .or_else(|| inner.second_chance.get_mut(&query_id));
        if let Some(context) = context
            && expected_execution.is_none_or(|execution| context.matches_execution(execution))
        {
            context.cancelled_by_fe = true;
        }
        RuntimeFilterQueryCancellationAction
    }

    fn execute_runtime_filter_query_cancellation(
        &self,
        _query_id: QueryId,
        _action: RuntimeFilterQueryCancellationAction,
    ) -> std::thread::Result<()> {
        Ok(())
    }

    #[allow(
        dead_code,
        reason = "Legacy abort remains available to non-lifecycle native fragment test fixtures."
    )]
    pub(crate) fn abort_query(&self, query_id: QueryId) -> Vec<UniqueId> {
        let (cancellation, finsts, exchange_ports) = {
            let mut guard = self.inner.lock().expect("query_ctx_manager lock");
            let cancellation =
                Self::prepare_runtime_filter_query_cancellation(&mut guard, query_id, None, None);
            let finsts = guard
                .finst_to_query
                .iter()
                .filter_map(|(finst_id, execution)| {
                    (execution.query_id() == query_id).then_some(*finst_id)
                })
                .collect::<Vec<_>>();
            let exchange_ports = finsts
                .iter()
                .filter_map(|finst| {
                    guard
                        .exchange_receiver_ports
                        .get(finst)
                        .map(|port| (*finst, Arc::clone(port)))
                })
                .collect::<Vec<_>>();
            (cancellation, finsts, exchange_ports)
        };
        if let Err(payload) = self.execute_runtime_filter_query_cancellation(query_id, cancellation)
        {
            std::panic::resume_unwind(payload);
        }
        for (fragment_instance_id, port) in exchange_ports {
            port.cancel_fragment(fragment_instance_id);
        }
        finsts
    }

    pub(crate) fn cancel_query(&self, query_id: QueryId, err: String) -> Vec<UniqueId> {
        let (cancellation, finsts, exchange_ports) = {
            let mut guard = self.inner.lock().expect("query_ctx_manager lock");
            let cancellation = Self::prepare_runtime_filter_query_cancellation(
                &mut guard,
                query_id,
                None,
                Some(&err),
            );

            let finsts = guard
                .finst_to_query
                .iter()
                .filter_map(|(finst_id, execution)| {
                    (execution.query_id() == query_id).then_some(*finst_id)
                })
                .collect::<Vec<_>>();
            let exchange_ports = finsts
                .iter()
                .filter_map(|finst| {
                    guard
                        .exchange_receiver_ports
                        .get(finst)
                        .map(|port| (*finst, Arc::clone(port)))
                })
                .collect::<Vec<_>>();
            (cancellation, finsts, exchange_ports)
        };

        let cancellation_unwind =
            self.execute_runtime_filter_query_cancellation(query_id, cancellation);
        if let Err(payload) = cancellation_unwind {
            std::panic::resume_unwind(payload);
        }
        for (fragment_instance_id, port) in exchange_ports {
            port.cancel_fragment(fragment_instance_id);
        }
        finsts
    }

    pub(crate) fn cancel_query_execution(
        &self,
        execution: QueryExecutionKey,
        err: String,
    ) -> Vec<UniqueId> {
        let (cancellation, finsts, detached_native_context) = {
            let mut guard = self.inner.lock().expect("query_ctx_manager lock");
            let query_id = execution.query_id();
            let cancellation = Self::prepare_runtime_filter_query_cancellation(
                &mut guard,
                query_id,
                Some(execution),
                Some(&err),
            );
            let finsts = guard
                .finst_to_query
                .iter()
                .filter_map(|(finst_id, current)| (*current == execution).then_some(*finst_id))
                .collect::<Vec<_>>();
            let detached_native_context = (execution.native_attempt_id().is_some()
                && finsts.is_empty()
                && guard.active.get(&query_id).is_some_and(|context| {
                    context.matches_execution(execution) && context.num_active_fragments == 0
                }))
            .then(|| guard.active.remove(&query_id))
            .flatten();
            (cancellation, finsts, detached_native_context)
        };
        let cancellation_unwind =
            self.execute_runtime_filter_query_cancellation(execution.query_id(), cancellation);
        if let Err(payload) = cancellation_unwind {
            std::panic::resume_unwind(payload);
        }
        drop(detached_native_context);
        finsts
    }

    pub(crate) fn cancel_finst(&self, finst_id: UniqueId, err: String) -> FinstCancelResult {
        self.cancel_finst_internal(finst_id, err, || {})
    }

    fn cancel_finst_internal<F>(
        &self,
        finst_id: UniqueId,
        err: String,
        binding_observer: F,
    ) -> FinstCancelResult
    where
        F: FnOnce(),
    {
        let collected = {
            let mut guard = self.inner.lock().expect("query_ctx_manager lock");
            let Some(execution) = guard.finst_to_query.get(&finst_id).copied() else {
                return FinstCancelResult {
                    query_id: None,
                    finsts: Vec::new(),
                };
            };
            binding_observer();
            let query_id = execution.query_id();
            let cancellation = Self::prepare_runtime_filter_query_cancellation(
                &mut guard,
                query_id,
                Some(execution),
                Some(&err),
            );
            let finsts = guard
                .finst_to_query
                .iter()
                .filter_map(|(finst_id, current)| (*current == execution).then_some(*finst_id))
                .collect::<Vec<_>>();
            let exchange_ports = finsts
                .iter()
                .filter_map(|finst| {
                    guard
                        .exchange_receiver_ports
                        .get(finst)
                        .map(|port| (*finst, Arc::clone(port)))
                })
                .collect::<Vec<_>>();
            (query_id, cancellation, finsts, exchange_ports)
        };
        let (query_id, cancellation, finsts, exchange_ports) = collected;
        let cancellation_unwind =
            self.execute_runtime_filter_query_cancellation(query_id, cancellation);
        if let Err(payload) = cancellation_unwind {
            std::panic::resume_unwind(payload);
        }
        for (fragment_instance_id, port) in exchange_ports {
            port.cancel_fragment(fragment_instance_id);
        }
        if finsts.is_empty() {
            return FinstCancelResult {
                query_id: Some(query_id),
                finsts,
            };
        }
        FinstCancelResult {
            query_id: Some(query_id),
            finsts,
        }
    }

    #[cfg(test)]
    fn cancel_finst_with_binding_observer<F>(
        &self,
        finst_id: UniqueId,
        err: String,
        binding_observer: F,
    ) -> FinstCancelResult
    where
        F: FnOnce(),
    {
        self.cancel_finst_internal(finst_id, err, binding_observer)
    }

    /// A sender's exchange RPC failed. Map the finst to its query and cancel
    /// the whole query so blocked receivers abort instead of timing out.
    pub(crate) fn propagate_sender_error(&self, finst_id: UniqueId, err: String) -> Vec<UniqueId> {
        let result = self.cancel_finst(finst_id, format!("exchange send failed: {err}"));
        match result.query_id {
            Some(_) => result.finsts,
            None => vec![finst_id],
        }
    }

    pub(crate) fn finish_fragment(&self, query_id: QueryId) {
        self.finish_fragment_internal(query_id)
    }

    pub(crate) fn finish_fragment_execution(&self, execution: QueryExecutionKey) {
        self.finish_fragment_internal_execution(execution.query_id(), Some(execution))
    }

    fn finish_fragment_internal(&self, query_id: QueryId) {
        self.finish_fragment_internal_execution(query_id, None)
    }

    fn finish_fragment_internal_execution(
        &self,
        query_id: QueryId,
        execution: Option<QueryExecutionKey>,
    ) {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        if execution.is_some_and(|execution| {
            !guard
                .active
                .get(&query_id)
                .is_some_and(|ctx| ctx.matches_execution(execution))
        }) {
            return;
        }
        let Some(mut ctx) = guard.active.remove(&query_id) else {
            return;
        };
        let no_active_fragments = ctx.count_down_fragments();
        if !no_active_fragments {
            guard.active.insert(query_id, ctx);
            return;
        }
        // Native lifecycle completion has already transferred its terminal fact
        // into the control plane. Do not retain the heavy execution context for
        // a legacy report-delivery retry window: a later attempt for the same
        // query id must be able to own the slot independently.
        if execution.is_some_and(|execution| execution.native_attempt_id().is_some()) {
            drop(guard);
            drop(ctx);
            return;
        }
        if ctx.is_dead() {
            drop(guard);
            drop(ctx);
            return;
        }
        ctx.extend_delivery_lifetime();
        guard.second_chance.insert(query_id, ctx);
    }
}

#[cfg(test)]
mod fragment_cancellation_boundary_tests {
    use novarocks_execution::exec::pipeline::global_driver_executor::FragmentCompletion;
    use novarocks_types::UniqueId;

    use super::{QueryContextManager, QueryId};

    #[test]
    fn query_cancellation_returns_routes_without_aborting_fragment_local_completion() {
        let manager = QueryContextManager::new_for_test();
        let query_id = QueryId::new(86_101, 86_102);
        let finst_id = UniqueId::new(86_103, 86_104);
        let completion = FragmentCompletion::new(1);
        manager.register_finst(finst_id, query_id);

        assert_eq!(
            manager.cancel_query(query_id, "query owner cancellation".to_string()),
            vec![finst_id]
        );
        assert!(
            !completion.should_abort(),
            "query owners must route cancellation through the role adapter instead of mutating fragment completion"
        );
        assert!(completion.driver_finished());
        assert_eq!(completion.wait(), Ok(()));
    }
}

static QUERY_CONTEXT_MANAGER: OnceLock<Arc<QueryContextManager>> = OnceLock::new();

pub(crate) fn query_context_manager() -> Arc<QueryContextManager> {
    QUERY_CONTEXT_MANAGER
        .get_or_init(QueryContextManager::new)
        .clone()
}

#[cfg(test)]
mod sender_error_tests {
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    use super::{QueryContextManager, QueryContextManagerInner, QueryId};
    use novarocks_types::UniqueId;

    fn test_manager() -> QueryContextManager {
        QueryContextManager {
            inner: Mutex::new(QueryContextManagerInner::default()),
            stopped: AtomicBool::new(false),
        }
    }

    #[test]
    fn mapped_finst_cancels_all_query_finsts_and_receivers() {
        let mgr = test_manager();
        let qid = QueryId::new(11, 22);
        let finst_a = UniqueId::new(101, 201);
        let finst_b = UniqueId::new(102, 202);

        mgr.get_or_register(qid, false, Duration::from_secs(1), Duration::from_secs(5))
            .expect("query context must be created");
        mgr.register_finst(finst_a, qid);
        mgr.register_finst(finst_b, qid);

        let mut finsts = mgr.propagate_sender_error(finst_a, "connection refused".into());
        finsts.sort_by_key(|id| (id.high(), id.low()));

        assert_eq!(finsts, vec![finst_a, finst_b]);
        assert!(mgr.is_query_canceled(qid));
    }

    #[test]
    fn unmapped_finst_cancels_its_own_receiver_only() {
        let mgr = test_manager();
        let finst = UniqueId::new(201, 202);

        let finsts = mgr.propagate_sender_error(finst, "broken pipe".into());

        assert_eq!(finsts, vec![finst]);
    }
}

#[cfg(test)]
mod native_lifecycle_cleanup_tests {
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    use super::{QueryContextManager, QueryContextManagerInner, QueryId};
    use novarocks_types::UniqueId;

    fn test_manager() -> QueryContextManager {
        QueryContextManager {
            inner: Mutex::new(QueryContextManagerInner::default()),
            stopped: AtomicBool::new(false),
        }
    }

    #[test]
    fn pre_start_registration_rollback_preserves_other_fragment_and_query_cleanup() {
        let mgr = test_manager();
        let query_id = QueryId::new(4_131, 4_132);
        let first = UniqueId::new(4_133, 1);
        let second = UniqueId::new(4_133, 2);

        mgr.ensure_native_context(
            query_id,
            false,
            Duration::from_secs(1),
            Duration::from_secs(5),
        )
        .expect("create native context");
        for finst_id in [first, second] {
            mgr.get_or_register_native(
                query_id,
                false,
                Duration::from_secs(1),
                Duration::from_secs(5),
            )
            .expect("register native fragment");
            mgr.register_finst(finst_id, query_id);
        }

        assert!(mgr.rollback_pre_ready_native_fragment(query_id, second));
        assert_eq!(mgr.fragment_counts_for_test(query_id), Some((1, 1)));
        assert_eq!(mgr.query_id_by_finst(first), Some(query_id));
        assert_eq!(mgr.query_id_by_finst(second), None);
        mgr.inner
            .lock()
            .expect("query ctx manager lock")
            .active
            .get_mut(&query_id)
            .expect("remaining fragment query")
            .total_fragments = Some(1);

        mgr.finish_fragment(query_id);
        mgr.unregister_finst(first);
        assert_eq!(mgr.fragment_counts_for_test(query_id), None);
        assert_eq!(mgr.query_id_by_finst(first), None);
    }
}

#[cfg(test)]
mod incremental_scan_domain_tests {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;

    use super::{QueryContextManager, QueryContextManagerInner, QueryId};
    use novarocks_execution::exec::node::scan::IncrementalScanRange;
    use novarocks_types::SlotId;
    use novarocks_types::UniqueId;

    fn manager() -> QueryContextManager {
        QueryContextManager {
            inner: Mutex::new(QueryContextManagerInner::default()),
            stopped: AtomicBool::new(false),
        }
    }

    #[test]
    fn pending_incremental_ranges_store_domain_values_and_registered_slot_contract() {
        let manager = manager();
        let finst_id = UniqueId::new(91, 92);
        manager.register_finsts_with_incremental_contracts(
            [(finst_id, HashMap::from([(41, Some(SlotId::new(7)))]))],
            QueryId::new(81, 82),
        );

        assert_eq!(
            manager
                .incremental_change_op_slot(finst_id, 41)
                .expect("registered contract"),
            Some(SlotId::new(7))
        );
        manager
            .append_incremental_scan_ranges(
                finst_id,
                41,
                vec![IncrementalScanRange::Empty {
                    has_more: Some(true),
                }],
            )
            .expect("queue domain range");
        let pending = manager.pending_incremental_scan_ranges_for_test(finst_id, 41);
        assert!(matches!(
            pending.as_slice(),
            [IncrementalScanRange::Empty {
                has_more: Some(true)
            }]
        ));
    }

    #[test]
    fn incremental_slot_lookup_rejects_unknown_node_without_pending_side_effect() {
        let manager = manager();
        let finst_id = UniqueId::new(93, 94);
        manager.register_finsts_with_incremental_contracts(
            [(finst_id, HashMap::from([(41, None)]))],
            QueryId::new(83, 84),
        );

        let error = manager
            .incremental_change_op_slot(finst_id, 42)
            .expect_err("unknown node must fail before append");
        assert!(error.contains("no registered scan contract"), "{error}");
        assert!(
            manager
                .pending_incremental_scan_ranges_for_test(finst_id, 42)
                .is_empty()
        );
    }
}

#[cfg(test)]
mod tests {}
