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

use crate::cache::CacheOptions;
use crate::common::ids::SlotId;
use crate::exec::node::scan::ConnectorRowPositionLookup;
use crate::exec::node::scan::IncrementalScanRange;
use crate::exec::node::scan::ScanOp;
use crate::exec::operators::scan::dispatch::ScanDispatchState;
use crate::exec::row_position::RowPositionDescriptor;
use crate::runtime::descriptor_snapshot::DescriptorSnapshot;
use crate::runtime::mem_tracker::{self, MemTracker};
use novarocks_types::UniqueId;

pub(crate) use novarocks_types::QueryId;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum QueryExecutionGeneration {
    Native(NonZeroU64),
}

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueryContextGeneration {
    Native(NonZeroU64),
}

pub struct QueryCleanupLease {
    release: Option<Box<dyn FnOnce() + Send + 'static>>,
}

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

pub(crate) struct QueryContext {
    #[allow(dead_code)]
    pub(crate) query_id: QueryId,
    execution_generation: QueryContextGeneration,
    pub(crate) cache_options: Option<CacheOptions>,
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
    pub(crate) row_pos_descs: HashMap<i32, RowPositionDescriptor>,
    pub(crate) lookup_fetchers: HashMap<i32, LookupFetcherLifecycle>,
    pub(crate) connector_glm_contexts: HashMap<SlotId, ConnectorRowPositionLookup>,
    pub(crate) mem_tracker: Arc<MemTracker>,
    cleanup_leases: Vec<QueryCleanupLease>,
}

#[derive(Default)]
struct RuntimeFilterQueryCancellationAction;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LookupFetcherLifecycle {
    Exact(usize),
    Unknown,
}

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
            cache_options: None,
            desc_snapshot: None,
            num_fragments: 0,
            num_active_fragments: 0,
            total_fragments: None,
            cancelled_by_fe: false,
            delivery_expire,
            delivery_deadline: now + delivery_expire,
            query_expire,
            query_deadline: now + query_expire,
            row_pos_descs: HashMap::new(),
            lookup_fetchers: HashMap::new(),
            connector_glm_contexts: HashMap::new(),
            mem_tracker,
            cleanup_leases: Vec::new(),
        }
    }

    fn matches_execution(&self, key: QueryExecutionKey) -> bool {
        self.query_id == key.query_id
            && matches!(
                (self.execution_generation, key.generation),
                (
                    QueryContextGeneration::Native(_),
                    QueryExecutionGeneration::Native(_)
                )
            )
            && match (self.execution_generation, key.generation) {
                (
                    QueryContextGeneration::Native(current),
                    QueryExecutionGeneration::Native(requested),
                ) => current == requested,
                _ => false,
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
            && self
                .lookup_fetchers
                .values()
                .all(|lifecycle| matches!(lifecycle, LookupFetcherLifecycle::Exact(0)))
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

    pub(crate) fn merge_row_pos_descs(
        &mut self,
        descs: HashMap<i32, RowPositionDescriptor>,
    ) -> Result<(), String> {
        self.validate_row_pos_descs(&descs)?;
        for (tuple_id, incoming) in descs {
            self.row_pos_descs.entry(tuple_id).or_insert(incoming);
        }
        Ok(())
    }

    fn validate_row_pos_descs(
        &self,
        descs: &HashMap<i32, RowPositionDescriptor>,
    ) -> Result<(), String> {
        for (tuple_id, incoming) in descs {
            if let Some(existing) = self.row_pos_descs.get(tuple_id) {
                if existing.row_position_type != incoming.row_position_type
                    || existing.row_source_slot != incoming.row_source_slot
                    || existing.fetch_ref_slots != incoming.fetch_ref_slots
                    || existing.lookup_ref_slots != incoming.lookup_ref_slots
                {
                    return Err(format!(
                        "conflicting row position descriptor for tuple_id={tuple_id}"
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn row_pos_desc(&self, tuple_id: i32) -> Option<RowPositionDescriptor> {
        self.row_pos_descs.get(&tuple_id).cloned()
    }

    pub(crate) fn register_lookup_fetchers(
        &mut self,
        lifecycles: &HashMap<i32, LookupFetcherLifecycle>,
    ) {
        for (node_id, incoming) in lifecycles {
            self.lookup_fetchers
                .entry(*node_id)
                .and_modify(|existing| {
                    *existing = match (*existing, *incoming) {
                        (
                            LookupFetcherLifecycle::Exact(current),
                            LookupFetcherLifecycle::Exact(new),
                        ) => LookupFetcherLifecycle::Exact(current.max(new)),
                        (LookupFetcherLifecycle::Unknown, LookupFetcherLifecycle::Exact(new)) => {
                            LookupFetcherLifecycle::Exact(new)
                        }
                        (
                            LookupFetcherLifecycle::Exact(current),
                            LookupFetcherLifecycle::Unknown,
                        ) => LookupFetcherLifecycle::Exact(current),
                        (LookupFetcherLifecycle::Unknown, LookupFetcherLifecycle::Unknown) => {
                            LookupFetcherLifecycle::Unknown
                        }
                    };
                })
                .or_insert(*incoming);
        }
    }

    pub(crate) fn complete_lookup_fetcher(&mut self, node_id: i32) -> Result<(), String> {
        let lifecycle = self
            .lookup_fetchers
            .get_mut(&node_id)
            .ok_or_else(|| format!("lookup node {node_id} is not registered"))?;
        let LookupFetcherLifecycle::Exact(count) = lifecycle else {
            // Without the FE-provided peer-fragment count, a close cannot prove that
            // it is the last fetch fragment. Keep the dispatcher until bounded expiry.
            return Ok(());
        };
        if *count == 0 {
            return Ok(());
        }
        *count -= 1;
        Ok(())
    }

    pub(crate) fn register_connector_glm(
        &mut self,
        row_source_slot: SlotId,
        lookup: ConnectorRowPositionLookup,
    ) -> Result<(), String> {
        if let Some(existing) = self.connector_glm_contexts.get(&row_source_slot) {
            if existing.binding.key() != lookup.binding.key() || existing.splits != lookup.splits {
                return Err(format!(
                    "conflicting connector late-materialization binding for row source slot {row_source_slot}"
                ));
            }
            return Ok(());
        }
        self.connector_glm_contexts.insert(row_source_slot, lookup);
        Ok(())
    }

    pub(crate) fn connector_glm_split(
        &self,
        row_source_slot: SlotId,
        scan_range_id: i32,
    ) -> Option<(
        Arc<novarocks_spi::connector::ConnectorExecutionBinding>,
        novarocks_spi::connector::ConnectorSplit,
    )> {
        let lookup = self.connector_glm_contexts.get(&row_source_slot)?;
        Some((
            Arc::clone(&lookup.binding),
            lookup.splits.get(&scan_range_id)?.clone(),
        ))
    }

    pub(crate) fn mem_tracker(&self) -> Arc<MemTracker> {
        Arc::clone(&self.mem_tracker)
    }

    pub(crate) fn set_cache_options(&mut self, options: CacheOptions) -> Result<(), String> {
        if let Some(existing) = self.cache_options.as_ref() {
            if existing != &options {
                return Err("cache options mismatch for query".to_string());
            }
            return Ok(());
        }
        self.cache_options = Some(options);
        Ok(())
    }

    pub(crate) fn cache_options(&self) -> Option<CacheOptions> {
        self.cache_options.clone()
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct FragmentFinishReportDecision {
    pub(crate) include_runtime_filter_profile: bool,
}

pub(crate) struct FinstCancelResult {
    pub(crate) query_id: Option<QueryId>,
    pub(crate) finsts: Vec<UniqueId>,
}

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

    pub(crate) fn set_cache_options(
        &self,
        query_id: QueryId,
        options: CacheOptions,
    ) -> Result<(), String> {
        self.with_context_mut(query_id, |ctx| ctx.set_cache_options(options))
    }

    pub(crate) fn set_cache_options_execution(
        &self,
        execution: QueryExecutionKey,
        options: CacheOptions,
    ) -> Result<(), String> {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        let ctx = guard
            .active
            .get_mut(&execution.query_id())
            .ok_or_else(|| "QueryContext not found".to_string())?;
        if !ctx.matches_execution(execution) {
            return Err("query execution generation is not active".to_string());
        }
        ctx.set_cache_options(options)
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

    pub(crate) fn cache_options(&self, query_id: QueryId) -> Option<CacheOptions> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .and_then(|ctx| ctx.cache_options())
            .or_else(|| {
                guard
                    .second_chance
                    .get(&query_id)
                    .and_then(|ctx| ctx.cache_options())
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

    pub(crate) fn register_row_pos_descs(
        &self,
        query_id: QueryId,
        descs: HashMap<i32, RowPositionDescriptor>,
    ) -> Result<(), String> {
        self.with_context_mut(query_id, |ctx| ctx.merge_row_pos_descs(descs))
    }

    pub(crate) fn register_lookup_fetchers(
        &self,
        query_id: QueryId,
        lifecycles: HashMap<i32, LookupFetcherLifecycle>,
    ) -> Result<(), String> {
        self.with_context_mut(query_id, |ctx| {
            ctx.register_lookup_fetchers(&lifecycles);
            Ok(())
        })
    }

    pub(crate) fn complete_lookup_fetcher(
        &self,
        query_id: QueryId,
        node_id: i32,
    ) -> Result<(), String> {
        let removed = {
            let mut guard = self.inner.lock().expect("query_ctx_manager lock");
            if let Some(ctx) = guard.active.get_mut(&query_id) {
                ctx.complete_lookup_fetcher(node_id)?;
                None
            } else if let Some(ctx) = guard.second_chance.get_mut(&query_id) {
                ctx.complete_lookup_fetcher(node_id)?;
                if ctx.is_dead() {
                    guard.second_chance.remove(&query_id)
                } else {
                    None
                }
            } else {
                return Err(format!("QueryContext not found: query_id={query_id}"));
            }
        };
        drop(removed);
        Ok(())
    }

    pub(crate) fn register_connector_glm(
        &self,
        query_id: QueryId,
        row_source_slot: SlotId,
        lookup: ConnectorRowPositionLookup,
    ) -> Result<(), String> {
        self.with_context_mut(query_id, |ctx| {
            ctx.register_connector_glm(row_source_slot, lookup)
        })
    }

    pub(crate) fn connector_glm_split(
        &self,
        query_id: QueryId,
        row_source_slot: SlotId,
        scan_range_id: i32,
    ) -> Option<(
        Arc<novarocks_spi::connector::ConnectorExecutionBinding>,
        novarocks_spi::connector::ConnectorSplit,
    )> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .and_then(|ctx| ctx.connector_glm_split(row_source_slot, scan_range_id))
    }

    pub(crate) fn row_pos_desc(
        &self,
        query_id: QueryId,
        tuple_id: i32,
    ) -> Option<RowPositionDescriptor> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .and_then(|ctx| ctx.row_pos_desc(tuple_id))
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
        RuntimeFilterQueryCancellationAction::default()
    }

    fn execute_runtime_filter_query_cancellation(
        &self,
        _query_id: QueryId,
        _action: RuntimeFilterQueryCancellationAction,
    ) -> std::thread::Result<()> {
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn abort_query(&self, query_id: QueryId) -> Vec<UniqueId> {
        let (cancellation, finsts) = {
            let mut guard = self.inner.lock().expect("query_ctx_manager lock");
            let cancellation =
                Self::prepare_runtime_filter_query_cancellation(&mut guard, query_id, None, None);
            let finsts = guard
                .finst_to_query
                .iter()
                .filter_map(|(finst_id, execution)| {
                    (execution.query_id() == query_id).then_some(*finst_id)
                })
                .collect();
            (cancellation, finsts)
        };
        if let Err(payload) = self.execute_runtime_filter_query_cancellation(query_id, cancellation)
        {
            std::panic::resume_unwind(payload);
        }
        finsts
    }

    pub(crate) fn cancel_query(&self, query_id: QueryId, err: String) -> Vec<UniqueId> {
        let (cancellation, finsts) = {
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
                .collect();
            (cancellation, finsts)
        };

        let cancellation_unwind =
            self.execute_runtime_filter_query_cancellation(query_id, cancellation);
        if let Err(payload) = cancellation_unwind {
            std::panic::resume_unwind(payload);
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
            (query_id, cancellation, finsts)
        };
        let (query_id, cancellation, finsts) = collected;
        let cancellation_unwind =
            self.execute_runtime_filter_query_cancellation(query_id, cancellation);
        if let Err(payload) = cancellation_unwind {
            std::panic::resume_unwind(payload);
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
            Some(_) => {
                let finsts = result.finsts;
                for id in &finsts {
                    crate::runtime::exchange::cancel_fragment(id.high(), id.low());
                }
                finsts
            }
            None => {
                crate::runtime::exchange::cancel_fragment(finst_id.high(), finst_id.low());
                vec![finst_id]
            }
        }
    }

    pub(crate) fn finish_fragment(&self, query_id: QueryId) {
        let decision = self.finish_fragment_internal(query_id);
        let _ = decision;
    }

    pub(crate) fn finish_fragment_for_report(
        &self,
        query_id: QueryId,
    ) -> FragmentFinishReportDecision {
        self.finish_fragment_internal(query_id)
    }

    pub(crate) fn finish_fragment_execution(&self, execution: QueryExecutionKey) {
        let decision =
            self.finish_fragment_internal_execution(execution.query_id(), Some(execution));
        let _ = decision;
    }

    pub(crate) fn finish_fragment_for_report_execution(
        &self,
        execution: QueryExecutionKey,
    ) -> FragmentFinishReportDecision {
        self.finish_fragment_internal_execution(execution.query_id(), Some(execution))
    }

    pub(crate) fn cleanup_after_fragment_report(
        &self,
        query_id: QueryId,
        decision: FragmentFinishReportDecision,
    ) {
        let _ = (query_id, decision);
    }

    fn finish_fragment_internal(&self, query_id: QueryId) -> FragmentFinishReportDecision {
        self.finish_fragment_internal_execution(query_id, None)
    }

    fn finish_fragment_internal_execution(
        &self,
        query_id: QueryId,
        execution: Option<QueryExecutionKey>,
    ) -> FragmentFinishReportDecision {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        if execution.is_some_and(|execution| {
            !guard
                .active
                .get(&query_id)
                .is_some_and(|ctx| ctx.matches_execution(execution))
        }) {
            return FragmentFinishReportDecision {
                include_runtime_filter_profile: false,
            };
        }
        let Some(mut ctx) = guard.active.remove(&query_id) else {
            return FragmentFinishReportDecision {
                include_runtime_filter_profile: true,
            };
        };
        let no_active_fragments = ctx.count_down_fragments();
        if !no_active_fragments {
            guard.active.insert(query_id, ctx);
            return FragmentFinishReportDecision::default();
        }
        // Native lifecycle completion has already transferred its terminal fact
        // into the control plane. Do not retain the heavy execution context for
        // a legacy report-delivery retry window: a later attempt for the same
        // query id must be able to own the slot independently.
        if execution.is_some_and(|execution| execution.native_attempt_id().is_some()) {
            let decision = FragmentFinishReportDecision {
                include_runtime_filter_profile: true,
            };
            drop(guard);
            drop(ctx);
            return decision;
        }
        if ctx.is_dead() {
            let decision = FragmentFinishReportDecision {
                include_runtime_filter_profile: true,
            };
            drop(guard);
            drop(ctx);
            return decision;
        }
        ctx.extend_delivery_lifetime();
        guard.second_chance.insert(query_id, ctx);
        FragmentFinishReportDecision {
            include_runtime_filter_profile: true,
        }
    }
}

#[cfg(test)]
mod fragment_cancellation_boundary_tests {
    use crate::common::types::UniqueId;
    use crate::exec::pipeline::global_driver_executor::FragmentCompletion;

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

#[cfg(test)]
mod lookup_lifecycle_tests {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    use super::{LookupFetcherLifecycle, QueryContextManager, QueryContextManagerInner, QueryId};

    fn test_manager() -> QueryContextManager {
        QueryContextManager {
            inner: Mutex::new(QueryContextManagerInner::default()),
            stopped: AtomicBool::new(false),
        }
    }

    #[test]
    fn lookup_context_survives_all_fragments_until_last_fetcher_closes() {
        let manager = test_manager();
        let query_id = QueryId::new(901, 902);
        for _ in 0..2 {
            manager
                .get_or_register(
                    query_id,
                    false,
                    Duration::from_secs(1),
                    Duration::from_secs(5),
                )
                .expect("fragment context");
        }
        {
            let mut guard = manager.inner.lock().expect("query ctx manager lock");
            guard
                .active
                .get_mut(&query_id)
                .expect("active query")
                .total_fragments = Some(2);
        }
        manager
            .register_lookup_fetchers(
                query_id,
                HashMap::from([(3, LookupFetcherLifecycle::Exact(1))]),
            )
            .expect("lookup lifecycle");

        manager.finish_fragment(query_id);
        manager.finish_fragment(query_id);

        {
            let guard = manager.inner.lock().expect("query ctx manager lock");
            assert!(!guard.active.contains_key(&query_id));
            assert!(guard.second_chance.contains_key(&query_id));
        }

        manager
            .complete_lookup_fetcher(query_id, 3)
            .expect("last fetcher close");

        let guard = manager.inner.lock().expect("query ctx manager lock");
        assert!(!guard.active.contains_key(&query_id));
        assert!(!guard.second_chance.contains_key(&query_id));
    }

    #[test]
    fn duplicate_fragment_registration_does_not_double_lookup_fetchers() {
        let manager = test_manager();
        let query_id = QueryId::new(911, 912);
        manager
            .get_or_register(
                query_id,
                false,
                Duration::from_secs(1),
                Duration::from_secs(5),
            )
            .expect("query context");

        for _ in 0..2 {
            manager
                .register_lookup_fetchers(
                    query_id,
                    HashMap::from([(7, LookupFetcherLifecycle::Exact(2))]),
                )
                .expect("idempotent registration");
        }

        manager
            .complete_lookup_fetcher(query_id, 7)
            .expect("first close");
        manager
            .complete_lookup_fetcher(query_id, 7)
            .expect("second close");
        manager
            .complete_lookup_fetcher(query_id, 7)
            .expect("duplicate close is idempotent");
    }

    #[test]
    fn unknown_lookup_fetcher_count_keeps_context_until_bounded_expiry() {
        let manager = test_manager();
        let query_id = QueryId::new(921, 922);
        manager
            .get_or_register(query_id, false, Duration::ZERO, Duration::from_secs(5))
            .expect("query context");
        manager
            .register_lookup_fetchers(
                query_id,
                HashMap::from([(8, LookupFetcherLifecycle::Unknown)]),
            )
            .expect("unknown lookup lifecycle");

        manager.finish_fragment(query_id);
        manager
            .complete_lookup_fetcher(query_id, 8)
            .expect("unknown close is acknowledged conservatively");

        {
            let guard = manager.inner.lock().expect("query ctx manager lock");
            assert!(guard.second_chance.contains_key(&query_id));
        }
        manager.clean_expired();
        let guard = manager.inner.lock().expect("query ctx manager lock");
        assert!(!guard.second_chance.contains_key(&query_id));
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
    use crate::common::types::UniqueId;
    use crate::runtime::exchange::{ExchangeKey, set_expected_senders, snapshot_receiver_state};

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
        let key_a = ExchangeKey {
            finst_id_hi: finst_a.high(),
            finst_id_lo: finst_a.low(),
            node_id: 301,
        };
        let key_b = ExchangeKey {
            finst_id_hi: finst_b.high(),
            finst_id_lo: finst_b.low(),
            node_id: 302,
        };

        mgr.get_or_register(qid, false, Duration::from_secs(1), Duration::from_secs(5))
            .expect("query context must be created");
        mgr.register_finst(finst_a, qid);
        mgr.register_finst(finst_b, qid);
        set_expected_senders(key_a, 1);
        set_expected_senders(key_b, 1);

        assert!(snapshot_receiver_state(key_a).is_some());
        assert!(snapshot_receiver_state(key_b).is_some());

        let mut finsts = mgr.propagate_sender_error(finst_a, "connection refused".into());
        finsts.sort_by_key(|id| (id.high(), id.low()));

        assert_eq!(finsts, vec![finst_a, finst_b]);
        assert!(mgr.is_query_canceled(qid));
        assert!(snapshot_receiver_state(key_a).is_none());
        assert!(snapshot_receiver_state(key_b).is_none());
    }

    #[test]
    fn unmapped_finst_cancels_its_own_receiver_only() {
        let mgr = test_manager();
        let finst = UniqueId::new(201, 202);
        let key = ExchangeKey {
            finst_id_hi: finst.high(),
            finst_id_lo: finst.low(),
            node_id: 401,
        };

        set_expected_senders(key, 1);
        assert!(snapshot_receiver_state(key).is_some());

        let finsts = mgr.propagate_sender_error(finst, "broken pipe".into());

        assert_eq!(finsts, vec![finst]);
        assert!(snapshot_receiver_state(key).is_none());
    }
}

#[cfg(test)]
mod runtime_filter_lifecycle_cleanup_tests {
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::time::{Duration, Instant};

    use super::{
        FragmentFinishReportDecision, QueryContext, QueryContextManager, QueryContextManagerInner,
        QueryId,
    };
    use crate::common::types::UniqueId;
    use crate::runtime::runtime_filter_observability::{QueryKey, RuntimeFilterLifecycleRegistry};

    fn test_manager() -> QueryContextManager {
        QueryContextManager {
            inner: Mutex::new(QueryContextManagerInner::default()),
            stopped: AtomicBool::new(false),
        }
    }

    #[test]
    fn finish_fragment_removes_runtime_filter_lifecycle_when_query_is_dead() {
        let mgr = test_manager();
        let query_id = QueryId::new(4_101, 4_102);
        let query_key = QueryKey::from_hi_lo(query_id.high(), query_id.low());
        let registry = RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);
        registry.recorder(query_key).planned(7);

        mgr.get_or_register(
            query_id,
            false,
            Duration::from_secs(1),
            Duration::from_secs(5),
        )
        .expect("query context must be created");
        {
            let mut guard = mgr.inner.lock().expect("query ctx manager lock");
            guard
                .active
                .get_mut(&query_id)
                .expect("active query")
                .total_fragments = Some(1);
        }

        mgr.finish_fragment(query_id);

        assert!(registry.snapshot(query_key).is_none());
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

        let decision = mgr.finish_fragment_for_report(query_id);
        mgr.unregister_finst(first);
        mgr.cleanup_after_fragment_report(query_id, decision);
        assert_eq!(mgr.fragment_counts_for_test(query_id), None);
        assert_eq!(mgr.query_id_by_finst(first), None);
        RuntimeFilterLifecycleRegistry::global()
            .remove_query(QueryKey::from_hi_lo(query_id.high(), query_id.low()));
    }

    #[test]
    fn finish_fragment_for_report_claims_runtime_filter_export_once_before_cleanup() {
        let mgr = test_manager();
        let query_id = QueryId::new(4_151, 4_152);
        let query_key = QueryKey::from_hi_lo(query_id.high(), query_id.low());
        let registry = RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);
        registry.recorder(query_key).planned(7);

        mgr.get_or_register(
            query_id,
            false,
            Duration::from_secs(1),
            Duration::from_secs(5),
        )
        .expect("first query context fragment must be created");
        mgr.get_or_register(
            query_id,
            false,
            Duration::from_secs(1),
            Duration::from_secs(5),
        )
        .expect("second query context fragment must be created");
        {
            let mut guard = mgr.inner.lock().expect("query ctx manager lock");
            guard
                .active
                .get_mut(&query_id)
                .expect("active query")
                .total_fragments = Some(2);
        }

        let first = mgr.finish_fragment_for_report(query_id);
        assert_eq!(first, FragmentFinishReportDecision::default());
        mgr.cleanup_after_fragment_report(query_id, first);
        assert!(registry.snapshot(query_key).is_some());

        let second = mgr.finish_fragment_for_report(query_id);
        assert!(second.include_runtime_filter_profile);
        assert!(registry.snapshot(query_key).is_some());

        mgr.cleanup_after_fragment_report(query_id, second);

        assert!(registry.snapshot(query_key).is_none());
    }

    #[test]
    fn report_cleanup_preserves_lifecycle_for_recreated_context() {
        let mgr = test_manager();
        let query_id = QueryId::new(4_181, 4_182);
        let query_key = QueryKey::from_hi_lo(query_id.high(), query_id.low());
        let registry = RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);

        mgr.get_or_register(
            query_id,
            false,
            Duration::from_secs(1),
            Duration::from_secs(5),
        )
        .expect("old query context");
        {
            let mut guard = mgr.inner.lock().expect("query ctx manager lock");
            guard
                .active
                .get_mut(&query_id)
                .expect("active query")
                .total_fragments = Some(1);
        }

        let decision = mgr.finish_fragment_for_report(query_id);
        mgr.ensure_context(
            query_id,
            false,
            Duration::from_secs(1),
            Duration::from_secs(5),
        )
        .expect("replacement query context");

        mgr.cleanup_after_fragment_report(query_id, decision);

        assert!(
            registry.snapshot(query_key).is_some(),
            "old report cleanup must preserve the replacement context lifecycle"
        );
        registry.remove_query(query_key);
    }

    #[test]
    fn clean_expired_removes_runtime_filter_lifecycle_for_second_chance_query() {
        let mgr = test_manager();
        let query_id = QueryId::new(4_201, 4_202);
        let query_key = QueryKey::from_hi_lo(query_id.high(), query_id.low());
        let registry = RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);
        registry.recorder(query_key).planned(7);

        let mut ctx = QueryContext::new(query_id, Duration::from_millis(1), Duration::from_secs(5));
        ctx.delivery_deadline = Instant::now() - Duration::from_millis(1);
        {
            let mut guard = mgr.inner.lock().expect("query ctx manager lock");
            guard.second_chance.insert(query_id, ctx);
        }

        mgr.clean_expired();

        assert!(registry.snapshot(query_key).is_none());
    }
}

#[cfg(test)]
mod incremental_scan_domain_tests {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;

    use super::{QueryContextManager, QueryContextManagerInner, QueryId};
    use crate::common::ids::SlotId;
    use crate::common::types::UniqueId;
    use crate::exec::node::scan::IncrementalScanRange;

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
