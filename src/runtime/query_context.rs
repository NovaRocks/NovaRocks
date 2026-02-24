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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::thread;
use std::time::{Duration, Instant};

use crate::cache::CacheOptions;
use crate::common::ids::SlotId;
use crate::common::types::UniqueId;
use crate::descriptors;
use crate::exec::node::scan::RowPositionScanConfig;
use crate::exec::pipeline::dependency::DependencyManager;
use crate::exec::pipeline::global_driver_executor::FragmentCompletion;
use crate::exec::row_position::RowPositionDescriptor;
use crate::fs::scan_context::FileScanRange;
use crate::internal_service;
use crate::runtime::lookup::GlobalLateMaterializationContext;
use crate::runtime::mem_tracker::{self, MemTracker};
use crate::runtime::runtime_filter_hub::RuntimeFilterHub;
use crate::runtime::runtime_filter_worker::RuntimeFilterWorker;
use crate::runtime_filter;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct QueryId {
    pub(crate) hi: i64,
    pub(crate) lo: i64,
}

pub(crate) struct QueryContext {
    #[allow(dead_code)]
    pub(crate) query_id: QueryId,
    pub(crate) cache_options: Option<CacheOptions>,
    pub(crate) desc_tbl: Option<descriptors::TDescriptorTable>,
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
    pub(crate) exchange_senders: HashMap<i32, usize>,
    pub(crate) runtime_filter_hub: Option<Arc<RuntimeFilterHub>>,
    pub(crate) runtime_filter_params: Option<runtime_filter::TRuntimeFilterParams>,
    pub(crate) runtime_filter_worker: Option<Arc<RuntimeFilterWorker>>,
    pub(crate) pending_runtime_filters: Vec<PendingRuntimeFilter>,
    pub(crate) row_pos_descs: HashMap<i32, RowPositionDescriptor>,
    pub(crate) glm_contexts: HashMap<SlotId, GlobalLateMaterializationContext>,
    pub(crate) lake_tablet_paths: HashMap<String, HashMap<i64, String>>,
    pub(crate) mem_tracker: Arc<MemTracker>,
}

#[derive(Clone, Debug)]
pub(crate) struct PendingRuntimeFilter {
    pub(crate) filter_id: i32,
    pub(crate) build_be_number: i32,
    pub(crate) data: Vec<u8>,
}

impl QueryContext {
    pub(crate) fn new(
        query_id: QueryId,
        delivery_expire: Duration,
        query_expire: Duration,
    ) -> Self {
        let now = Instant::now();
        let process = mem_tracker::process_mem_tracker();
        let query_label = format!("query_{:x}_{:x}", query_id.hi, query_id.lo);
        let mem_tracker = MemTracker::new_child(query_label, &process);
        Self {
            query_id,
            cache_options: None,
            desc_tbl: None,
            num_fragments: 0,
            num_active_fragments: 0,
            total_fragments: None,
            cancelled_by_fe: false,
            delivery_expire,
            delivery_deadline: now + delivery_expire,
            query_expire,
            query_deadline: now + query_expire,
            exchange_senders: HashMap::new(),
            runtime_filter_hub: None,
            runtime_filter_params: None,
            runtime_filter_worker: None,
            pending_runtime_filters: Vec::new(),
            row_pos_descs: HashMap::new(),
            glm_contexts: HashMap::new(),
            lake_tablet_paths: HashMap::new(),
            mem_tracker,
        }
    }

    pub(crate) fn increment_num_fragments(&mut self) {
        self.num_fragments += 1;
        self.num_active_fragments += 1;
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

    #[allow(dead_code)]
    pub(crate) fn is_query_expired(&self) -> bool {
        Instant::now() >= self.query_deadline
    }

    pub(crate) fn extend_delivery_lifetime(&mut self) {
        self.delivery_deadline = Instant::now() + self.delivery_expire;
    }

    pub(crate) fn update_exchange_senders(&mut self, counts: HashMap<i32, usize>) {
        for (node_id, count) in counts {
            let entry = self.exchange_senders.entry(node_id).or_insert(0);
            if *entry < count {
                *entry = count;
            }
        }
    }

    pub(crate) fn exchange_sender_count(&self, node_id: i32) -> Option<usize> {
        self.exchange_senders.get(&node_id).copied()
    }

    pub(crate) fn set_runtime_filter_hub(&mut self, hub: Arc<RuntimeFilterHub>) {
        self.runtime_filter_hub = Some(hub);
    }

    pub(crate) fn runtime_filter_hub(&self) -> Option<Arc<RuntimeFilterHub>> {
        self.runtime_filter_hub.clone()
    }

    pub(crate) fn set_runtime_filter_params(
        &mut self,
        params: runtime_filter::TRuntimeFilterParams,
    ) {
        if self.runtime_filter_params.is_none() {
            self.runtime_filter_params = Some(params);
        }
    }

    pub(crate) fn runtime_filter_params(&self) -> Option<runtime_filter::TRuntimeFilterParams> {
        self.runtime_filter_params.clone()
    }

    pub(crate) fn set_runtime_filter_worker(&mut self, worker: Arc<RuntimeFilterWorker>) {
        self.runtime_filter_worker = Some(worker);
    }

    pub(crate) fn runtime_filter_worker(&self) -> Option<Arc<RuntimeFilterWorker>> {
        self.runtime_filter_worker.clone()
    }

    pub(crate) fn set_row_pos_descs(&mut self, descs: HashMap<i32, RowPositionDescriptor>) {
        self.row_pos_descs = descs;
    }

    pub(crate) fn row_pos_desc(&self, tuple_id: i32) -> Option<RowPositionDescriptor> {
        self.row_pos_descs.get(&tuple_id).cloned()
    }

    pub(crate) fn register_glm_scan_ranges(
        &mut self,
        row_source_slot: SlotId,
        scan_cfg: RowPositionScanConfig,
        ranges: Vec<FileScanRange>,
    ) {
        let ctx = self
            .glm_contexts
            .entry(row_source_slot)
            .or_insert_with(|| GlobalLateMaterializationContext::new(row_source_slot, scan_cfg));
        ctx.register_ranges(ranges);
    }

    pub(crate) fn glm_scan_range(
        &self,
        row_source_slot: SlotId,
        scan_range_id: i32,
    ) -> Option<FileScanRange> {
        self.glm_contexts
            .get(&row_source_slot)
            .and_then(|ctx| ctx.get_scan_range(scan_range_id).cloned())
    }

    pub(crate) fn glm_scan_config(&self, row_source_slot: SlotId) -> Option<RowPositionScanConfig> {
        self.glm_contexts
            .get(&row_source_slot)
            .map(|ctx| ctx.scan_config.clone())
    }

    pub(crate) fn push_pending_runtime_filter(
        &mut self,
        filter_id: i32,
        build_be_number: i32,
        data: Vec<u8>,
    ) {
        self.pending_runtime_filters.push(PendingRuntimeFilter {
            filter_id,
            build_be_number,
            data,
        });
    }

    pub(crate) fn drain_pending_runtime_filters(&mut self) -> Vec<PendingRuntimeFilter> {
        std::mem::take(&mut self.pending_runtime_filters)
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

    pub(crate) fn set_lake_tablet_paths(&mut self, cache_key: String, paths: HashMap<i64, String>) {
        self.lake_tablet_paths.insert(cache_key, paths);
    }

    pub(crate) fn lake_tablet_paths(&self, cache_key: &str) -> Option<HashMap<i64, String>> {
        self.lake_tablet_paths.get(cache_key).cloned()
    }
}

#[derive(Default)]
struct QueryContextManagerInner {
    active: HashMap<QueryId, QueryContext>,
    second_chance: HashMap<QueryId, QueryContext>,
    finst_to_query: HashMap<UniqueId, QueryId>,
    fragment_completions: HashMap<UniqueId, Weak<FragmentCompletion>>,
}

pub(crate) struct QueryContextManager {
    inner: Mutex<QueryContextManagerInner>,
    stopped: AtomicBool,
}

impl QueryContextManager {
    fn new() -> Arc<Self> {
        let manager = Arc::new(Self {
            inner: Mutex::new(QueryContextManagerInner::default()),
            stopped: AtomicBool::new(false),
        });
        let mgr = Arc::clone(&manager);
        thread::spawn(move || mgr.clean_loop());
        manager
    }

    fn clean_loop(self: Arc<Self>) {
        while !self.stopped.load(Ordering::Relaxed) {
            self.clean_expired();
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn clean_expired(&self) {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        let mut to_remove = Vec::new();
        for (qid, ctx) in &guard.second_chance {
            if ctx.has_no_active_instances() && ctx.is_delivery_expired() {
                to_remove.push(*qid);
            }
        }
        for qid in to_remove {
            guard.second_chance.remove(&qid);
        }
    }

    pub(crate) fn get_or_register(
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

    pub(crate) fn ensure_context(
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
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        if let Some(ctx) = guard.active.get_mut(&query_id) {
            if increment {
                ctx.increment_num_fragments();
            }
            return Ok(());
        }
        if let Some(mut ctx) = guard.second_chance.remove(&query_id) {
            if increment {
                ctx.increment_num_fragments();
            }
            guard.active.insert(query_id, ctx);
            return Ok(());
        }
        if return_error_if_not_exist {
            return Err("Query terminates prematurely (missing QueryContext)".to_string());
        }
        let mut ctx = QueryContext::new(query_id, delivery_expire, query_expire);
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

    pub(crate) fn set_lake_tablet_paths(
        &self,
        query_id: QueryId,
        cache_key: String,
        paths: HashMap<i64, String>,
    ) -> Result<(), String> {
        self.with_context_mut(query_id, |ctx| {
            ctx.set_lake_tablet_paths(cache_key, paths);
            Ok(())
        })
    }

    pub(crate) fn lake_tablet_paths(
        &self,
        query_id: QueryId,
        cache_key: &str,
    ) -> Option<HashMap<i64, String>> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .and_then(|ctx| ctx.lake_tablet_paths(cache_key))
    }

    pub(crate) fn update_exchange_sender_counts(
        &self,
        query_id: QueryId,
        counts: HashMap<i32, usize>,
    ) -> Result<(), String> {
        self.with_context_mut(query_id, |ctx| {
            ctx.update_exchange_senders(counts);
            Ok(())
        })
    }

    pub(crate) fn register_row_pos_descs(
        &self,
        query_id: QueryId,
        descs: HashMap<i32, RowPositionDescriptor>,
    ) -> Result<(), String> {
        self.with_context_mut(query_id, |ctx| {
            ctx.set_row_pos_descs(descs);
            Ok(())
        })
    }

    pub(crate) fn register_glm_scan_ranges(
        &self,
        query_id: QueryId,
        row_source_slot: SlotId,
        scan_cfg: RowPositionScanConfig,
        ranges: Vec<FileScanRange>,
    ) -> Result<(), String> {
        self.with_context_mut(query_id, |ctx| {
            ctx.register_glm_scan_ranges(row_source_slot, scan_cfg, ranges);
            Ok(())
        })
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

    pub(crate) fn glm_scan_range(
        &self,
        query_id: QueryId,
        row_source_slot: SlotId,
        scan_range_id: i32,
    ) -> Option<FileScanRange> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .and_then(|ctx| ctx.glm_scan_range(row_source_slot, scan_range_id))
    }

    pub(crate) fn glm_scan_config(
        &self,
        query_id: QueryId,
        row_source_slot: SlotId,
    ) -> Option<RowPositionScanConfig> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .and_then(|ctx| ctx.glm_scan_config(row_source_slot))
    }

    pub(crate) fn exchange_sender_count(&self, query_id: QueryId, node_id: i32) -> Option<usize> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .and_then(|ctx| ctx.exchange_sender_count(node_id))
    }

    pub(crate) fn query_mem_tracker(&self, query_id: QueryId) -> Option<Arc<MemTracker>> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .map(|ctx| ctx.mem_tracker())
    }

    pub(crate) fn desc_tbl(&self, query_id: QueryId) -> Option<descriptors::TDescriptorTable> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .and_then(|ctx| ctx.desc_tbl.clone())
    }

    pub(crate) fn set_runtime_filter_hub(
        &self,
        query_id: QueryId,
        hub: Arc<RuntimeFilterHub>,
    ) -> Result<(), String> {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        if let Some(ctx) = guard.active.get_mut(&query_id) {
            ctx.set_runtime_filter_hub(hub);
            return Ok(());
        }
        if let Some(ctx) = guard.second_chance.get_mut(&query_id) {
            ctx.set_runtime_filter_hub(hub);
            return Ok(());
        }
        Err("QueryContext not found".to_string())
    }

    pub(crate) fn get_runtime_filter_hub(
        &self,
        query_id: QueryId,
    ) -> Option<Arc<RuntimeFilterHub>> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .and_then(|ctx| ctx.runtime_filter_hub())
    }

    pub(crate) fn set_runtime_filter_params(
        &self,
        query_id: QueryId,
        params: runtime_filter::TRuntimeFilterParams,
    ) -> Result<(), String> {
        let pending = self.with_context_mut(query_id, |ctx| {
            ctx.set_runtime_filter_params(params);
            Ok(ctx.drain_pending_runtime_filters())
        })?;
        if let Some(worker) = self.get_or_create_runtime_filter_worker(query_id) {
            for item in pending {
                let _ = worker.receive_partial(item.filter_id, &item.data, item.build_be_number);
            }
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn get_runtime_filter_params(
        &self,
        query_id: QueryId,
    ) -> Option<runtime_filter::TRuntimeFilterParams> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .and_then(|ctx| ctx.runtime_filter_params())
    }

    #[allow(dead_code)]
    pub(crate) fn set_runtime_filter_worker(
        &self,
        query_id: QueryId,
        worker: Arc<RuntimeFilterWorker>,
    ) -> Result<(), String> {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        if let Some(ctx) = guard.active.get_mut(&query_id) {
            ctx.set_runtime_filter_worker(worker);
            return Ok(());
        }
        if let Some(ctx) = guard.second_chance.get_mut(&query_id) {
            ctx.set_runtime_filter_worker(worker);
            return Ok(());
        }
        Err("QueryContext not found".to_string())
    }

    #[allow(dead_code)]
    pub(crate) fn get_runtime_filter_worker(
        &self,
        query_id: QueryId,
    ) -> Option<Arc<RuntimeFilterWorker>> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .and_then(|ctx| ctx.runtime_filter_worker())
    }

    pub(crate) fn get_or_create_runtime_filter_worker(
        &self,
        query_id: QueryId,
    ) -> Option<Arc<RuntimeFilterWorker>> {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        let ctx = if let Some(ctx) = guard.active.get_mut(&query_id) {
            ctx
        } else if let Some(ctx) = guard.second_chance.get_mut(&query_id) {
            ctx
        } else {
            return None;
        };
        if let Some(worker) = ctx.runtime_filter_worker() {
            return Some(worker);
        }
        let params = ctx.runtime_filter_params()?;
        let hub = if let Some(hub) = ctx.runtime_filter_hub() {
            hub
        } else {
            let hub = Arc::new(RuntimeFilterHub::new(DependencyManager::new()));
            ctx.set_runtime_filter_hub(Arc::clone(&hub));
            hub
        };
        let worker = Arc::new(RuntimeFilterWorker::new(query_id, params, hub));
        ctx.set_runtime_filter_worker(Arc::clone(&worker));
        Some(worker)
    }

    pub(crate) fn enqueue_pending_runtime_filter(
        &self,
        query_id: QueryId,
        filter_id: i32,
        build_be_number: i32,
        data: Vec<u8>,
    ) -> Result<(), String> {
        self.with_context_mut(query_id, |ctx| {
            ctx.push_pending_runtime_filter(filter_id, build_be_number, data);
            Ok(())
        })
    }

    pub(crate) fn register_finst(&self, finst_id: UniqueId, query_id: QueryId) {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        guard.finst_to_query.insert(finst_id, query_id);
    }

    pub(crate) fn unregister_finst(&self, finst_id: UniqueId) {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        guard.finst_to_query.remove(&finst_id);
        guard.fragment_completions.remove(&finst_id);
    }

    pub(crate) fn register_fragment_completion(
        &self,
        finst_id: UniqueId,
        completion: Arc<FragmentCompletion>,
    ) {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .fragment_completions
            .insert(finst_id, Arc::downgrade(&completion));
    }

    pub(crate) fn unregister_fragment_completion(&self, finst_id: UniqueId) {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        guard.fragment_completions.remove(&finst_id);
    }

    pub(crate) fn get_query_timeout_by_finst(&self, finst_id: UniqueId) -> Option<Duration> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        let query_id = guard.finst_to_query.get(&finst_id).copied()?;
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .map(|ctx| ctx.query_expire)
    }

    pub(crate) fn is_query_canceled(&self, query_id: QueryId) -> bool {
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

    #[allow(dead_code)]
    pub(crate) fn abort_query(&self, query_id: QueryId) -> Vec<UniqueId> {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        if let Some(ctx) = guard.active.get_mut(&query_id) {
            ctx.cancelled_by_fe = true;
        }
        if let Some(ctx) = guard.second_chance.get_mut(&query_id) {
            ctx.cancelled_by_fe = true;
        }
        guard
            .finst_to_query
            .iter()
            .filter_map(|(finst_id, qid)| (*qid == query_id).then_some(*finst_id))
            .collect()
    }

    pub(crate) fn cancel_query(&self, query_id: QueryId, err: String) -> Vec<UniqueId> {
        let (finsts, completions) = {
            let mut guard = self.inner.lock().expect("query_ctx_manager lock");
            if let Some(ctx) = guard.active.get_mut(&query_id) {
                ctx.cancelled_by_fe = true;
            }
            if let Some(ctx) = guard.second_chance.get_mut(&query_id) {
                ctx.cancelled_by_fe = true;
            }

            let mut finsts = Vec::new();
            let mut completions = Vec::new();
            let mut stale = Vec::new();
            for (finst_id, qid) in guard.finst_to_query.iter() {
                if *qid != query_id {
                    continue;
                }
                finsts.push(*finst_id);
                if let Some(weak) = guard.fragment_completions.get(finst_id) {
                    if let Some(completion) = weak.upgrade() {
                        completions.push(completion);
                    } else {
                        stale.push(*finst_id);
                    }
                }
            }
            for finst_id in stale {
                guard.fragment_completions.remove(&finst_id);
            }
            (finsts, completions)
        };

        for completion in completions {
            completion.abort_from_query(err.clone());
        }
        finsts
    }

    pub(crate) fn finish_fragment(&self, query_id: QueryId) {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        let Some(mut ctx) = guard.active.remove(&query_id) else {
            return;
        };
        ctx.count_down_fragments();
        if ctx.is_dead() {
            return;
        }
        if ctx.has_no_active_instances() {
            ctx.extend_delivery_lifetime();
            guard.second_chance.insert(query_id, ctx);
        } else {
            guard.active.insert(query_id, ctx);
        }
    }
}

static QUERY_CONTEXT_MANAGER: OnceLock<Arc<QueryContextManager>> = OnceLock::new();

pub(crate) fn query_context_manager() -> Arc<QueryContextManager> {
    QUERY_CONTEXT_MANAGER
        .get_or_init(QueryContextManager::new)
        .clone()
}

pub(crate) fn observe_total_fragments(
    ctx: &mut QueryContext,
    exec_params: &internal_service::TPlanFragmentExecParams,
) {
    if let Some(n) = exec_params.instances_number {
        let n = n.max(0) as usize;
        ctx.total_fragments = Some(ctx.total_fragments.map_or(n, |cur| cur.max(n)));
    }
}

pub(crate) fn desc_tbl_is_cached(desc: &descriptors::TDescriptorTable) -> bool {
    desc.is_cached.unwrap_or(false)
}

pub(crate) fn is_desc_tbl_effectively_empty(desc: &descriptors::TDescriptorTable) -> bool {
    let has_tuple = !desc.tuple_descriptors.is_empty();
    let has_table = desc
        .table_descriptors
        .as_ref()
        .is_some_and(|v| !v.is_empty());
    let has_slot = desc
        .slot_descriptors
        .as_ref()
        .is_some_and(|v| !v.is_empty());
    !(has_tuple || has_table || has_slot)
}

pub(crate) fn resolve_desc_tbl_for_instance(
    mgr: &QueryContextManager,
    query_id: QueryId,
    incoming: Option<&descriptors::TDescriptorTable>,
    fallback: Option<&descriptors::TDescriptorTable>,
) -> Result<Option<descriptors::TDescriptorTable>, String> {
    mgr.with_context_mut(query_id, |ctx| {
        if let Some(desc) = incoming {
            if desc_tbl_is_cached(desc) {
                return ctx
                    .desc_tbl
                    .clone()
                    .ok_or_else(|| "Query terminates prematurely (missing desc_tbl)".to_string())
                    .map(Some);
            }
            if !is_desc_tbl_effectively_empty(desc) {
                ctx.desc_tbl = Some(desc.clone());
                return Ok(Some(desc.clone()));
            }
        }
        if let Some(desc) = fallback {
            if !is_desc_tbl_effectively_empty(desc) {
                ctx.desc_tbl = Some(desc.clone());
                return Ok(Some(desc.clone()));
            }
        }
        Ok(ctx.desc_tbl.clone())
    })
}

pub(crate) fn query_expire_durations(
    query_opts: Option<&internal_service::TQueryOptions>,
) -> (Duration, Duration) {
    let default_timeout = 300i32;
    let query_timeout = query_opts
        .and_then(|o| o.query_timeout)
        .unwrap_or(default_timeout)
        .max(1);
    let delivery_timeout = query_opts
        .and_then(|o| o.query_delivery_timeout)
        .map(|v| v.max(1).min(query_timeout))
        .unwrap_or(query_timeout);
    (
        Duration::from_secs(delivery_timeout as u64),
        Duration::from_secs(query_timeout as u64),
    )
}
