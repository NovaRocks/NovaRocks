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
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use crate::cache::CacheOptions;
use crate::common::ids::SlotId;
use crate::common::types::UniqueId;
#[cfg(feature = "compat")]
use crate::exec::node::scan::IncrementalScanRange;
#[cfg(feature = "compat")]
use crate::exec::node::scan::LakeGlmScanInfo;
use crate::exec::node::scan::RowPositionScanConfig;
#[cfg(feature = "compat")]
use crate::exec::node::scan::ScanOp;
#[cfg(feature = "compat")]
use crate::exec::operators::scan::dispatch::ScanDispatchState;
use crate::exec::row_position::RowPositionDescriptor;
use crate::fs::scan_context::FileScanRange;
use crate::protocol::native::RuntimeFilterQueryLifecycleOptions;
use crate::runtime::descriptor_snapshot::DescriptorSnapshot;
use crate::runtime::lookup::GlobalLateMaterializationContext;
use crate::runtime::mem_tracker::{self, MemTracker};
pub(crate) use crate::runtime::query_options::query_expire_durations;
use crate::runtime::runtime_filter_observability::{
    QueryKey, RegistryRuntimeFilterEventSink, RuntimeFilterLifecycleRegistry,
};
use crate::runtime_filter::model::policy::MAX_DEADLINE_MS;
use crate::runtime_filter::port::identity::DeploymentEpoch;
use crate::runtime_filter::port::install::RuntimeFilterParticipantInstall;
use crate::runtime_filter::port::producer::{InstallContractErrorKind, InstallOutcome};
use crate::runtime_filter::service::{
    NativeRuntimeFilterExecutionContext, ReliableTransportPolicy, RuntimeFilterService,
};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct QueryId {
    pub(crate) hi: i64,
    pub(crate) lo: i64,
}

impl QueryId {
    pub const fn new(hi: i64, lo: i64) -> Self {
        Self { hi, lo }
    }

    pub const fn hi(self) -> i64 {
        self.hi
    }

    pub const fn lo(self) -> i64 {
        self.lo
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct StarRocksQueryGeneration(NonZeroU64);

impl StarRocksQueryGeneration {
    pub(crate) fn new(generation: u64) -> Result<Self, String> {
        NonZeroU64::new(generation)
            .map(Self)
            .ok_or_else(|| "StarRocks query generation must be non-zero".to_string())
    }

    pub(crate) const fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum QueryExecutionGeneration {
    Native,
    StarRocks(StarRocksQueryGeneration),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct QueryExecutionKey {
    query_id: QueryId,
    generation: QueryExecutionGeneration,
}

impl QueryExecutionKey {
    pub(crate) const fn native(query_id: QueryId) -> Self {
        Self {
            query_id,
            generation: QueryExecutionGeneration::Native,
        }
    }

    pub(crate) const fn starrocks(query_id: QueryId, generation: StarRocksQueryGeneration) -> Self {
        Self {
            query_id,
            generation: QueryExecutionGeneration::StarRocks(generation),
        }
    }

    pub(crate) const fn query_id(self) -> QueryId {
        self.query_id
    }

    pub(crate) const fn starrocks_generation(self) -> Option<StarRocksQueryGeneration> {
        match self.generation {
            QueryExecutionGeneration::Native => None,
            QueryExecutionGeneration::StarRocks(generation) => Some(generation),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueryContextGeneration {
    Native,
    StarRocksUnbound,
    StarRocks(StarRocksQueryGeneration),
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

impl fmt::Display for QueryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let uuid = crate::common::types::format_uuid(self.hi, self.lo);
        f.write_str(&uuid)
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
    runtime_filter_cancel_retain_until: Option<Instant>,
    pub(crate) delivery_expire: Duration,
    pub(crate) delivery_deadline: Instant,
    #[allow(dead_code)]
    pub(crate) query_expire: Duration,
    #[allow(dead_code)]
    pub(crate) query_deadline: Instant,
    runtime_filter_service: Arc<RuntimeFilterService>,
    runtime_filter_deployment: Option<RuntimeFilterDeploymentClaim>,
    pub(crate) row_pos_descs: HashMap<i32, RowPositionDescriptor>,
    pub(crate) lookup_fetchers: HashMap<i32, LookupFetcherLifecycle>,
    pub(crate) glm_contexts: HashMap<SlotId, GlobalLateMaterializationContext>,
    #[cfg(feature = "compat")]
    pub(crate) lake_glm_contexts: HashMap<SlotId, LakeGlmScanInfo>,
    pub(crate) lake_tablet_paths: HashMap<String, HashMap<i64, String>>,
    pub(crate) mem_tracker: Arc<MemTracker>,
    #[cfg(feature = "compat")]
    starrocks_preparing_admissions: usize,
    cleanup_leases: Vec<QueryCleanupLease>,
}

#[derive(Clone)]
struct RuntimeFilterDeploymentClaim {
    state: RuntimeFilterDeploymentClaimState,
    lifecycle: RuntimeFilterQueryLifecycleOptions,
    install: RuntimeFilterParticipantInstall,
    completion: Arc<RuntimeFilterDeploymentCompletion>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RuntimeFilterDeploymentClaimState {
    Installing,
    Installed,
}

struct RuntimeFilterDeploymentCompletion {
    complete: Mutex<bool>,
    changed: Condvar,
}

impl RuntimeFilterDeploymentCompletion {
    fn new() -> Self {
        Self {
            complete: Mutex::new(false),
            changed: Condvar::new(),
        }
    }

    fn wait(&self) {
        let mut complete = self
            .complete
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        while !*complete {
            complete = self
                .changed
                .wait(complete)
                .unwrap_or_else(|error| error.into_inner());
        }
    }

    fn finish(&self) {
        *self
            .complete
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = true;
        self.changed.notify_all();
    }
}

enum RuntimeFilterDeploymentTerminalState {
    Aborting(Arc<RuntimeFilterDeploymentCompletion>),
    Aborted,
}

struct RuntimeFilterDeploymentTerminalRecord {
    epoch: DeploymentEpoch,
    state: RuntimeFilterDeploymentTerminalState,
    expires_at: Instant,
}

struct RuntimeFilterQueryCancellationRecord {
    expires_at: Instant,
}

struct RuntimeFilterQueryCancellationTerminalization {
    epoch: DeploymentEpoch,
    completion: Arc<RuntimeFilterDeploymentCompletion>,
    install_completion: Arc<RuntimeFilterDeploymentCompletion>,
    after_shutdown: Option<Arc<dyn Fn() + Send + Sync>>,
}

#[derive(Default)]
struct RuntimeFilterQueryCancellationAction {
    service: Option<Arc<RuntimeFilterService>>,
    terminalization: Option<RuntimeFilterQueryCancellationTerminalization>,
}

// The control plane has no separate retry-horizon knob before Task 5. Retain an
// aborted deployment for the canonical maximum RF deadline, which is no shorter
// than any validated channel retry horizon. Capacity is explicit and fail-closed:
// live records are never evicted to admit a new query.
const RUNTIME_FILTER_CONTROL_RETRY_HORIZON: Duration = Duration::from_millis(MAX_DEADLINE_MS);
const MAX_RUNTIME_FILTER_DEPLOYMENT_RECORDS: usize = 65_536;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeFilterDeploymentInstallOutcome {
    Applied,
    Idempotent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeFilterDeploymentAbortOutcome {
    Applied,
    Idempotent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeFilterDeploymentInstallErrorKind {
    StaleEpoch,
    ConflictingDeployment,
    QueryAborted,
    ActiveFragments,
    TerminalCapacity,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterDeploymentInstallError {
    kind: RuntimeFilterDeploymentInstallErrorKind,
    detail: String,
}

impl RuntimeFilterDeploymentInstallError {
    fn new(kind: RuntimeFilterDeploymentInstallErrorKind, detail: impl Into<String>) -> Self {
        Self {
            kind,
            detail: detail.into(),
        }
    }

    pub(crate) const fn kind(&self) -> RuntimeFilterDeploymentInstallErrorKind {
        self.kind
    }

    pub(crate) fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for RuntimeFilterDeploymentInstallError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "runtime filter deployment {:?}: {}",
            self.kind, self.detail
        )
    }
}

impl std::error::Error for RuntimeFilterDeploymentInstallError {}

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
            QueryContextGeneration::Native,
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
        let query_label = format!("query_{:x}_{:x}", query_id.hi, query_id.lo);
        let mem_tracker = MemTracker::new_child(query_label, &process);
        let runtime_filter_service = new_runtime_filter_service(query_id, &mem_tracker);
        Self {
            query_id,
            execution_generation,
            cache_options: None,
            desc_snapshot: None,
            num_fragments: 0,
            num_active_fragments: 0,
            total_fragments: None,
            cancelled_by_fe: false,
            runtime_filter_cancel_retain_until: None,
            delivery_expire,
            delivery_deadline: now + delivery_expire,
            query_expire,
            query_deadline: now + query_expire,
            runtime_filter_service,
            runtime_filter_deployment: None,
            row_pos_descs: HashMap::new(),
            lookup_fetchers: HashMap::new(),
            glm_contexts: HashMap::new(),
            #[cfg(feature = "compat")]
            lake_glm_contexts: HashMap::new(),
            lake_tablet_paths: HashMap::new(),
            mem_tracker,
            #[cfg(feature = "compat")]
            starrocks_preparing_admissions: 0,
            cleanup_leases: Vec::new(),
        }
    }

    fn matches_execution(&self, key: QueryExecutionKey) -> bool {
        self.query_id == key.query_id
            && matches!(
                (self.execution_generation, key.generation),
                (
                    QueryContextGeneration::Native,
                    QueryExecutionGeneration::Native
                ) | (
                    QueryContextGeneration::StarRocks(_),
                    QueryExecutionGeneration::StarRocks(_)
                )
            )
            && match (self.execution_generation, key.generation) {
                (
                    QueryContextGeneration::StarRocks(current),
                    QueryExecutionGeneration::StarRocks(requested),
                ) => current == requested,
                (QueryContextGeneration::Native, QueryExecutionGeneration::Native) => true,
                _ => false,
            }
    }

    #[cfg(all(test, feature = "compat"))]
    fn bind_starrocks_generation(
        &mut self,
        generation: StarRocksQueryGeneration,
    ) -> Result<(), String> {
        match self.execution_generation {
            QueryContextGeneration::StarRocksUnbound if self.num_fragments == 0 => {
                self.execution_generation = QueryContextGeneration::StarRocks(generation);
                Ok(())
            }
            QueryContextGeneration::StarRocks(current) if current == generation => Ok(()),
            QueryContextGeneration::StarRocksUnbound => {
                Err("cannot bind StarRocks generation after fragment registration".to_string())
            }
            QueryContextGeneration::StarRocks(current) => Err(format!(
                "StarRocks query generation mismatch: current={} requested={}",
                current.get(),
                generation.get()
            )),
            QueryContextGeneration::Native => {
                Err("cannot bind StarRocks generation to native query context".to_string())
            }
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

    fn runtime_filter_deployment_is_installing(&self) -> bool {
        self.runtime_filter_deployment
            .as_ref()
            .is_some_and(|claim| claim.state == RuntimeFilterDeploymentClaimState::Installing)
    }

    fn runtime_filter_cancel_is_retained(&self, now: Instant) -> bool {
        self.runtime_filter_cancel_retain_until
            .is_some_and(|retain_until| retain_until > now)
    }

    fn runtime_filter_cancel_retention_expired(&self, now: Instant) -> bool {
        self.runtime_filter_cancel_retain_until
            .is_some_and(|retain_until| retain_until <= now)
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

    pub(crate) fn runtime_filter_service(&self) -> Arc<RuntimeFilterService> {
        self.runtime_filter_service.clone()
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

    #[cfg(feature = "compat")]
    pub(crate) fn register_lake_glm(&mut self, row_source_slot: SlotId, info: LakeGlmScanInfo) {
        self.lake_glm_contexts.insert(row_source_slot, info);
    }

    #[cfg(feature = "compat")]
    pub(crate) fn lake_glm_info(&self, row_source_slot: SlotId) -> Option<&LakeGlmScanInfo> {
        self.lake_glm_contexts.get(&row_source_slot)
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

fn new_runtime_filter_service(
    query_id: QueryId,
    mem_tracker: &Arc<MemTracker>,
) -> Arc<RuntimeFilterService> {
    let query_key = QueryKey::from_hi_lo(query_id.hi, query_id.lo);
    let event_sink = Arc::new(RegistryRuntimeFilterEventSink::new(
        RuntimeFilterLifecycleRegistry::global(),
        query_key,
    ));
    Arc::new(RuntimeFilterService::new_for_query(
        UniqueId {
            hi: query_id.hi,
            lo: query_id.lo,
        },
        event_sink,
        mem_tracker,
    ))
}

impl Drop for QueryContext {
    fn drop(&mut self) {
        self.runtime_filter_service.shutdown();
    }
}

#[cfg(feature = "compat")]
struct IncrementalScanNodeHandle {
    op: Arc<dyn ScanOp>,
    dispatch: Arc<ScanDispatchState>,
    update_mu: Mutex<()>,
}

#[cfg(feature = "compat")]
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
    runtime_filter_deployment_terminals: HashMap<QueryId, RuntimeFilterDeploymentTerminalRecord>,
    runtime_filter_query_cancellations: HashMap<QueryId, RuntimeFilterQueryCancellationRecord>,
    runtime_filter_query_cancellation_errors: HashMap<QueryId, String>,
    #[cfg(test)]
    runtime_filter_terminal_now: Option<Instant>,
    #[cfg(test)]
    runtime_filter_terminal_capacity: Option<usize>,
    #[cfg(test)]
    before_runtime_filter_service_install: Option<Arc<dyn Fn() + Send + Sync>>,
    #[cfg(test)]
    before_runtime_filter_installed_publish: Option<Arc<dyn Fn() + Send + Sync>>,
    #[cfg(test)]
    install_wait_observer: Option<Arc<dyn Fn() + Send + Sync>>,
    #[cfg(test)]
    after_runtime_filter_service_shutdown: Option<Arc<dyn Fn() + Send + Sync>>,
    #[cfg(test)]
    abort_wait_observer: Option<Arc<dyn Fn() + Send + Sync>>,
    #[cfg(feature = "compat")]
    incremental_scan_nodes: HashMap<UniqueId, HashMap<i32, Arc<IncrementalScanNodeHandle>>>,
    #[cfg(feature = "compat")]
    pending_incremental_scan_ranges: HashMap<UniqueId, HashMap<i32, Vec<IncrementalScanRange>>>,
    #[cfg(feature = "compat")]
    incremental_change_op_slots: HashMap<UniqueId, HashMap<i32, Option<SlotId>>>,
}

pub(crate) struct QueryContextManager {
    inner: Mutex<QueryContextManagerInner>,
    stopped: AtomicBool,
}

fn runtime_filter_terminal_now(_inner: &QueryContextManagerInner) -> Instant {
    #[cfg(test)]
    if let Some(now) = _inner.runtime_filter_terminal_now {
        return now;
    }
    Instant::now()
}

fn runtime_filter_terminal_capacity(_inner: &QueryContextManagerInner) -> usize {
    #[cfg(test)]
    if let Some(capacity) = _inner.runtime_filter_terminal_capacity {
        return capacity;
    }
    MAX_RUNTIME_FILTER_DEPLOYMENT_RECORDS
}

fn runtime_filter_deployment_record_count(inner: &QueryContextManagerInner) -> usize {
    inner.runtime_filter_deployment_terminals.len()
        + inner.runtime_filter_query_cancellations.len()
        + inner
            .active
            .iter()
            .filter(|(query_id, context)| {
                context.runtime_filter_deployment.is_some()
                    && !inner
                        .runtime_filter_deployment_terminals
                        .contains_key(query_id)
            })
            .count()
        + inner
            .second_chance
            .iter()
            .filter(|(query_id, context)| {
                context.runtime_filter_deployment.is_some()
                    && !inner
                        .runtime_filter_deployment_terminals
                        .contains_key(query_id)
            })
            .count()
}

fn runtime_filter_deployment_has_reserved_record(
    inner: &QueryContextManagerInner,
    query_id: QueryId,
) -> bool {
    inner
        .runtime_filter_deployment_terminals
        .contains_key(&query_id)
        || inner
            .runtime_filter_query_cancellations
            .contains_key(&query_id)
        || inner
            .active
            .get(&query_id)
            .or_else(|| inner.second_chance.get(&query_id))
            .is_some_and(|context| context.runtime_filter_deployment.is_some())
}

fn ensure_runtime_filter_deployment_record_capacity(
    inner: &QueryContextManagerInner,
    query_id: QueryId,
) -> Result<(), RuntimeFilterDeploymentInstallError> {
    if runtime_filter_deployment_has_reserved_record(inner, query_id)
        || runtime_filter_deployment_record_count(inner) < runtime_filter_terminal_capacity(inner)
    {
        return Ok(());
    }
    Err(RuntimeFilterDeploymentInstallError::new(
        RuntimeFilterDeploymentInstallErrorKind::TerminalCapacity,
        "runtime filter deployment terminal capacity is exhausted; live records are retained fail-closed",
    ))
}

fn clean_expired_runtime_filter_terminals(inner: &mut QueryContextManagerInner) {
    let now = runtime_filter_terminal_now(inner);
    inner
        .runtime_filter_deployment_terminals
        .retain(|_, record| {
            matches!(
                record.state,
                RuntimeFilterDeploymentTerminalState::Aborting(_)
            ) || record.expires_at > now
        });
    inner
        .runtime_filter_query_cancellations
        .retain(|_, record| record.expires_at > now);
    let retained_queries = inner
        .active
        .keys()
        .chain(inner.second_chance.keys())
        .chain(inner.runtime_filter_deployment_terminals.keys())
        .chain(inner.runtime_filter_query_cancellations.keys())
        .copied()
        .collect::<HashSet<_>>();
    inner
        .runtime_filter_query_cancellation_errors
        .retain(|query_id, _| retained_queries.contains(query_id));
}

fn runtime_filter_terminal_expiry(inner: &QueryContextManagerInner) -> Instant {
    runtime_filter_terminal_now(inner) + RUNTIME_FILTER_CONTROL_RETRY_HORIZON
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct FragmentFinishReportDecision {
    pub(crate) include_runtime_filter_profile: bool,
    remove_runtime_filter_lifecycle_after_report: bool,
}

pub(crate) struct FinstCancelResult {
    pub(crate) query_id: Option<QueryId>,
    pub(crate) finsts: Vec<UniqueId>,
}

#[cfg(feature = "compat")]
pub(crate) struct StarRocksQueryHandoff {
    pub(crate) execution: QueryExecutionKey,
    pub(crate) delivery_expire: Duration,
    pub(crate) query_expire: Duration,
    pub(crate) fragment_count: usize,
    pub(crate) cache_options: CacheOptions,
    pub(crate) descriptor_snapshot: Option<Arc<DescriptorSnapshot>>,
    pub(crate) total_fragments: Option<usize>,
    pub(crate) row_pos_descs: HashMap<i32, RowPositionDescriptor>,
    pub(crate) lookup_fetchers: HashMap<i32, LookupFetcherLifecycle>,
    pub(crate) instances: Vec<(UniqueId, HashMap<i32, Option<SlotId>>)>,
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

    #[cfg(test)]
    pub(crate) fn new_for_test() -> Arc<Self> {
        let mut inner = QueryContextManagerInner::default();
        inner.runtime_filter_terminal_now = Some(Instant::now());
        Arc::new(Self {
            inner: Mutex::new(inner),
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
        let (expired, active_services, runtime_filter_now) = {
            let mut guard = self.inner.lock().expect("query_ctx_manager lock");
            let runtime_filter_now = runtime_filter_terminal_now(&guard);
            clean_expired_runtime_filter_terminals(&mut guard);
            let expired_second_chance = guard
                .second_chance
                .iter()
                .filter_map(|(qid, ctx)| {
                    (ctx.has_no_active_instances()
                        && !ctx.runtime_filter_deployment_is_installing()
                        && (ctx.runtime_filter_cancel_retention_expired(runtime_filter_now)
                            || (!ctx.runtime_filter_cancel_is_retained(runtime_filter_now)
                                && ctx.is_delivery_expired())))
                    .then_some(*qid)
                })
                .collect::<Vec<_>>();
            let expired_active = guard
                .active
                .iter()
                .filter_map(|(qid, ctx)| {
                    (ctx.has_no_active_instances()
                        && !ctx.runtime_filter_deployment_is_installing()
                        && (ctx.runtime_filter_cancel_retention_expired(runtime_filter_now)
                            || (!ctx.runtime_filter_cancel_is_retained(runtime_filter_now)
                                && ctx.is_query_expired())))
                    .then_some(*qid)
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
            let active_services = guard
                .active
                .values()
                .chain(guard.second_chance.values())
                .map(QueryContext::runtime_filter_service)
                .collect::<Vec<_>>();
            (expired, active_services, runtime_filter_now)
        };
        for (qid, ctx) in expired {
            ctx.runtime_filter_service().shutdown();
            drop(ctx);
            self.remove_runtime_filter_lifecycle_if_context_absent(qid);
        }
        for service in active_services {
            service.tick(runtime_filter_now);
        }
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

    #[cfg(test)]
    pub(crate) fn runtime_filter_query_cancellation_error_for_test(
        &self,
        query_id: QueryId,
    ) -> Option<String> {
        self.inner
            .lock()
            .expect("query_ctx_manager lock")
            .runtime_filter_query_cancellation_errors
            .get(&query_id)
            .cloned()
    }

    #[cfg(test)]
    pub(crate) fn set_before_runtime_filter_service_install_hook_for_test(
        &self,
        hook: Arc<dyn Fn() + Send + Sync>,
    ) {
        self.inner
            .lock()
            .expect("query_ctx_manager lock")
            .before_runtime_filter_service_install = Some(hook);
    }

    #[cfg(test)]
    pub(crate) fn set_before_runtime_filter_installed_publish_hook_for_test(
        &self,
        hook: Arc<dyn Fn() + Send + Sync>,
    ) {
        self.inner
            .lock()
            .expect("query_ctx_manager lock")
            .before_runtime_filter_installed_publish = Some(hook);
    }

    #[cfg(test)]
    pub(crate) fn set_after_runtime_filter_service_shutdown_hook_for_test(
        &self,
        hook: Arc<dyn Fn() + Send + Sync>,
    ) {
        self.inner
            .lock()
            .expect("query_ctx_manager lock")
            .after_runtime_filter_service_shutdown = Some(hook);
    }

    #[cfg(test)]
    pub(crate) fn set_abort_wait_observer_for_test(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        self.inner
            .lock()
            .expect("query_ctx_manager lock")
            .abort_wait_observer = Some(hook);
    }

    #[cfg(test)]
    pub(crate) fn advance_runtime_filter_terminal_clock_for_test(&self, advance: Duration) {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        let now = guard
            .runtime_filter_terminal_now
            .expect("test manager owns deterministic terminal clock");
        guard.runtime_filter_terminal_now = Some(now + advance);
    }

    #[cfg(test)]
    pub(crate) fn set_runtime_filter_terminal_capacity_for_test(&self, capacity: usize) {
        assert!(capacity > 0, "terminal capacity must be nonzero");
        self.inner
            .lock()
            .expect("query_ctx_manager lock")
            .runtime_filter_terminal_capacity = Some(capacity);
    }

    #[cfg(test)]
    pub(crate) fn runtime_filter_terminal_count_for_test(&self) -> usize {
        self.inner
            .lock()
            .expect("query_ctx_manager lock")
            .runtime_filter_deployment_terminals
            .len()
    }

    #[cfg(test)]
    pub(crate) const fn runtime_filter_control_retry_horizon_for_test(&self) -> Duration {
        RUNTIME_FILTER_CONTROL_RETRY_HORIZON
    }

    #[cfg(test)]
    pub(crate) fn runtime_filter_deployment_is_installing_for_test(
        &self,
        query_id: QueryId,
    ) -> bool {
        self.runtime_filter_deployment_claim_state_for_test(query_id)
            == Some(RuntimeFilterDeploymentClaimState::Installing)
    }

    #[cfg(test)]
    pub(crate) fn runtime_filter_deployment_is_installed_for_test(
        &self,
        query_id: QueryId,
    ) -> bool {
        self.runtime_filter_deployment_claim_state_for_test(query_id)
            == Some(RuntimeFilterDeploymentClaimState::Installed)
    }

    #[cfg(test)]
    fn runtime_filter_deployment_claim_state_for_test(
        &self,
        query_id: QueryId,
    ) -> Option<RuntimeFilterDeploymentClaimState> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .and_then(|context| context.runtime_filter_deployment.as_ref())
            .map(|claim| claim.state)
    }

    #[cfg(test)]
    pub(crate) fn runtime_filter_deployment_is_aborting_for_test(&self, query_id: QueryId) -> bool {
        self.inner
            .lock()
            .expect("query_ctx_manager lock")
            .runtime_filter_deployment_terminals
            .get(&query_id)
            .is_some_and(|record| {
                matches!(
                    record.state,
                    RuntimeFilterDeploymentTerminalState::Aborting(_)
                )
            })
    }

    #[cfg(test)]
    pub(crate) fn runtime_filter_deployment_is_aborted_for_test(&self, query_id: QueryId) -> bool {
        self.inner
            .lock()
            .expect("query_ctx_manager lock")
            .runtime_filter_deployment_terminals
            .get(&query_id)
            .is_some_and(|record| {
                matches!(record.state, RuntimeFilterDeploymentTerminalState::Aborted)
            })
    }

    fn remove_runtime_filter_lifecycle_if_context_absent(&self, query_id: QueryId) {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        if !guard.active.contains_key(&query_id) && !guard.second_chance.contains_key(&query_id) {
            remove_runtime_filter_lifecycle(query_id);
        }
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
            QueryContextGeneration::Native,
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
            QueryContextGeneration::Native,
            true,
        )
    }

    pub(crate) fn install_runtime_filter_deployment(
        &self,
        query_id: QueryId,
        lifecycle: RuntimeFilterQueryLifecycleOptions,
        install: RuntimeFilterParticipantInstall,
    ) -> Result<RuntimeFilterDeploymentInstallOutcome, RuntimeFilterDeploymentInstallError> {
        enum InstallStep {
            Wait {
                completion: Arc<RuntimeFilterDeploymentCompletion>,
                observer: Option<Arc<dyn Fn() + Send + Sync>>,
            },
            Install {
                service: Arc<RuntimeFilterService>,
                completion: Arc<RuntimeFilterDeploymentCompletion>,
                just_created: bool,
                before_service_install: Option<Arc<dyn Fn() + Send + Sync>>,
                before_installed_publish: Option<Arc<dyn Fn() + Send + Sync>>,
            },
        }

        let epoch = install.epoch();
        let (service, completion, just_created, before_service_install, before_installed_publish) = loop {
            let step = {
                let mut guard = self.inner.lock().expect("query_ctx_manager lock");
                clean_expired_runtime_filter_terminals(&mut guard);
                if guard
                    .runtime_filter_query_cancellations
                    .contains_key(&query_id)
                {
                    return Err(RuntimeFilterDeploymentInstallError::new(
                        RuntimeFilterDeploymentInstallErrorKind::QueryAborted,
                        "runtime filter deployment query was already cancelled",
                    ));
                }
                if let Some(record) = guard.runtime_filter_deployment_terminals.get(&query_id) {
                    let kind = if epoch.get() < record.epoch.get() {
                        RuntimeFilterDeploymentInstallErrorKind::StaleEpoch
                    } else if epoch == record.epoch {
                        RuntimeFilterDeploymentInstallErrorKind::QueryAborted
                    } else {
                        RuntimeFilterDeploymentInstallErrorKind::ConflictingDeployment
                    };
                    return Err(RuntimeFilterDeploymentInstallError::new(
                        kind,
                        "runtime filter install epoch conflicts with the query terminal record",
                    ));
                }

                let location = if guard.active.contains_key(&query_id) {
                    Some(true)
                } else if guard.second_chance.contains_key(&query_id) {
                    Some(false)
                } else {
                    None
                };
                if location.is_some_and(|active| {
                    let context = if active {
                        guard.active.get(&query_id).expect("checked active")
                    } else {
                        guard
                            .second_chance
                            .get(&query_id)
                            .expect("checked second chance")
                    };
                    context.cancelled_by_fe
                }) {
                    return Err(RuntimeFilterDeploymentInstallError::new(
                        RuntimeFilterDeploymentInstallErrorKind::QueryAborted,
                        "runtime filter deployment query was already cancelled",
                    ));
                }
                let existing_claim = location.and_then(|active| {
                    let context = if active {
                        guard.active.get(&query_id).expect("checked active")
                    } else {
                        guard
                            .second_chance
                            .get(&query_id)
                            .expect("checked second chance")
                    };
                    context.runtime_filter_deployment.as_ref().map(|claim| {
                        (
                            claim.state,
                            claim.lifecycle,
                            claim.install.clone(),
                            claim.completion.clone(),
                        )
                    })
                });
                if let Some((state, current_lifecycle, current_install, completion)) =
                    existing_claim
                {
                    if epoch.get() < current_install.epoch().get() {
                        return Err(RuntimeFilterDeploymentInstallError::new(
                            RuntimeFilterDeploymentInstallErrorKind::StaleEpoch,
                            "runtime filter deployment epoch is older than the installed epoch",
                        ));
                    }
                    if epoch != current_install.epoch()
                        || lifecycle != current_lifecycle
                        || install != current_install
                    {
                        return Err(RuntimeFilterDeploymentInstallError::new(
                            RuntimeFilterDeploymentInstallErrorKind::ConflictingDeployment,
                            "runtime filter deployment conflicts with the query installation",
                        ));
                    }
                    if state == RuntimeFilterDeploymentClaimState::Installed {
                        return Ok(RuntimeFilterDeploymentInstallOutcome::Idempotent);
                    }
                    #[cfg(test)]
                    let observer = guard.install_wait_observer.clone();
                    #[cfg(not(test))]
                    let observer: Option<Arc<dyn Fn() + Send + Sync>> = None;
                    InstallStep::Wait {
                        completion,
                        observer,
                    }
                } else {
                    if let Some(active) = location {
                        let context = if active {
                            guard.active.get(&query_id).expect("checked active")
                        } else {
                            guard
                                .second_chance
                                .get(&query_id)
                                .expect("checked second chance")
                        };
                        if context.num_fragments != 0 || context.num_active_fragments != 0 {
                            return Err(RuntimeFilterDeploymentInstallError::new(
                                RuntimeFilterDeploymentInstallErrorKind::ActiveFragments,
                                "runtime filter deployment must be installed before fragment registration",
                            ));
                        }
                    }
                    ensure_runtime_filter_deployment_record_capacity(&guard, query_id)?;
                    if location.is_none() {
                        let context = QueryContext::new(
                            query_id,
                            lifecycle.delivery_expire,
                            lifecycle.query_expire,
                        );
                        guard.active.insert(query_id, context);
                    }

                    let context = if guard.active.contains_key(&query_id) {
                        guard.active.get_mut(&query_id).expect("checked active")
                    } else {
                        guard
                            .second_chance
                            .get_mut(&query_id)
                            .expect("checked second chance")
                    };
                    let completion = Arc::new(RuntimeFilterDeploymentCompletion::new());
                    context.runtime_filter_deployment = Some(RuntimeFilterDeploymentClaim {
                        state: RuntimeFilterDeploymentClaimState::Installing,
                        lifecycle,
                        install: install.clone(),
                        completion: completion.clone(),
                    });
                    let service = context.runtime_filter_service();
                    #[cfg(test)]
                    let before_service_install = guard.before_runtime_filter_service_install.take();
                    #[cfg(not(test))]
                    let before_service_install: Option<
                        Arc<dyn Fn() + Send + Sync>,
                    > = None;
                    #[cfg(test)]
                    let before_installed_publish =
                        guard.before_runtime_filter_installed_publish.take();
                    #[cfg(not(test))]
                    let before_installed_publish: Option<
                        Arc<dyn Fn() + Send + Sync>,
                    > = None;
                    InstallStep::Install {
                        service,
                        completion,
                        just_created: location.is_none(),
                        before_service_install,
                        before_installed_publish,
                    }
                }
            };
            match step {
                InstallStep::Wait {
                    completion,
                    observer,
                } => {
                    if let Some(observer) = observer {
                        observer();
                    }
                    completion.wait();
                }
                InstallStep::Install {
                    service,
                    completion,
                    just_created,
                    before_service_install,
                    before_installed_publish,
                } => {
                    break (
                        service,
                        completion,
                        just_created,
                        before_service_install,
                        before_installed_publish,
                    );
                }
            }
        };

        let install_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if let Some(hook) = before_service_install {
                hook();
            }
            service
                .configure_transport(ReliableTransportPolicy::new(
                    lifecycle.transport_retry_interval,
                    lifecycle.transport_max_attempts,
                    lifecycle.transport_deadline,
                    lifecycle.transport_max_pending_entries,
                    lifecycle.transport_max_pending_bytes,
                ))
                .map_err(|error| {
                    RuntimeFilterDeploymentInstallError::new(
                        RuntimeFilterDeploymentInstallErrorKind::Internal,
                        error,
                    )
                })?;
            let outcome = service.install(install.clone()).map_err(|error| {
                map_runtime_filter_install_error(error.kind(), error.to_string())
            })?;
            if let Some(hook) = before_installed_publish {
                hook();
            }
            Ok::<_, RuntimeFilterDeploymentInstallError>(outcome)
        }));
        let outcome = match install_result {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(error)) => {
                self.retire_failed_runtime_filter_deployment_install(
                    query_id,
                    epoch,
                    just_created,
                    &service,
                    &completion,
                );
                return Err(error);
            }
            Err(payload) => {
                self.retire_failed_runtime_filter_deployment_install(
                    query_id,
                    epoch,
                    just_created,
                    &service,
                    &completion,
                );
                std::panic::resume_unwind(payload);
            }
        };

        let publish_result = {
            let mut guard = self.inner.lock().expect("query_ctx_manager lock");
            (|| {
                if guard
                    .runtime_filter_deployment_terminals
                    .contains_key(&query_id)
                {
                    return Err(RuntimeFilterDeploymentInstallError::new(
                        RuntimeFilterDeploymentInstallErrorKind::QueryAborted,
                        "runtime filter deployment was aborted before install acknowledgement",
                    ));
                }
                let context = if guard.active.contains_key(&query_id) {
                    guard.active.get_mut(&query_id).expect("checked active")
                } else {
                    guard.second_chance.get_mut(&query_id).ok_or_else(|| {
                        RuntimeFilterDeploymentInstallError::new(
                            RuntimeFilterDeploymentInstallErrorKind::QueryAborted,
                            "runtime filter deployment context disappeared before acknowledgement",
                        )
                    })?
                };
                if !Arc::ptr_eq(&context.runtime_filter_service, &service) {
                    return Err(RuntimeFilterDeploymentInstallError::new(
                        RuntimeFilterDeploymentInstallErrorKind::QueryAborted,
                        "runtime filter deployment context changed before acknowledgement",
                    ));
                }
                if context.cancelled_by_fe {
                    return Err(RuntimeFilterDeploymentInstallError::new(
                        RuntimeFilterDeploymentInstallErrorKind::QueryAborted,
                        "runtime filter deployment query was cancelled before acknowledgement",
                    ));
                }
                let claim = context.runtime_filter_deployment.as_mut().ok_or_else(|| {
                    RuntimeFilterDeploymentInstallError::new(
                        RuntimeFilterDeploymentInstallErrorKind::QueryAborted,
                        "runtime filter deployment claim disappeared before acknowledgement",
                    )
                })?;
                if claim.install.epoch() != epoch
                    || claim.lifecycle != lifecycle
                    || claim.install != install
                    || !Arc::ptr_eq(&claim.completion, &completion)
                {
                    return Err(RuntimeFilterDeploymentInstallError::new(
                        RuntimeFilterDeploymentInstallErrorKind::ConflictingDeployment,
                        "runtime filter deployment claim changed before acknowledgement",
                    ));
                }
                claim.state = RuntimeFilterDeploymentClaimState::Installed;
                Ok(())
            })()
        };
        if let Err(error) = publish_result {
            self.retire_failed_runtime_filter_deployment_install(
                query_id,
                epoch,
                just_created,
                &service,
                &completion,
            );
            return Err(error);
        }
        completion.finish();
        Ok(match outcome {
            InstallOutcome::AlreadyInstalled => RuntimeFilterDeploymentInstallOutcome::Idempotent,
            InstallOutcome::Installed | InstallOutcome::IgnoredEmpty => {
                RuntimeFilterDeploymentInstallOutcome::Applied
            }
        })
    }

    fn retire_failed_runtime_filter_deployment_install(
        &self,
        query_id: QueryId,
        epoch: DeploymentEpoch,
        just_created: bool,
        service: &Arc<RuntimeFilterService>,
        install_completion: &Arc<RuntimeFilterDeploymentCompletion>,
    ) {
        let (completion, removed, after_shutdown) = {
            let mut guard = self.inner.lock().expect("query_ctx_manager lock");
            if let Some(record) = guard.runtime_filter_deployment_terminals.get(&query_id) {
                let wait = match &record.state {
                    RuntimeFilterDeploymentTerminalState::Aborting(completion) => {
                        Some(completion.clone())
                    }
                    RuntimeFilterDeploymentTerminalState::Aborted => None,
                };
                drop(guard);
                if let Some(completion) = wait {
                    completion.wait();
                }
                install_completion.finish();
                return;
            }
            let completion = Arc::new(RuntimeFilterDeploymentCompletion::new());
            let expires_at = runtime_filter_terminal_expiry(&guard);
            guard.runtime_filter_query_cancellations.remove(&query_id);
            guard.runtime_filter_deployment_terminals.insert(
                query_id,
                RuntimeFilterDeploymentTerminalRecord {
                    epoch,
                    state: RuntimeFilterDeploymentTerminalState::Aborting(completion.clone()),
                    expires_at,
                },
            );
            let matching_active = guard.active.get(&query_id).is_some_and(|context| {
                context.num_fragments == 0
                    && context.num_active_fragments == 0
                    && Arc::ptr_eq(&context.runtime_filter_service, service)
                    && context
                        .runtime_filter_deployment
                        .as_ref()
                        .is_some_and(|claim| Arc::ptr_eq(&claim.completion, install_completion))
            });
            let matching_second_chance =
                guard.second_chance.get(&query_id).is_some_and(|context| {
                    context.num_fragments == 0
                        && context.num_active_fragments == 0
                        && Arc::ptr_eq(&context.runtime_filter_service, service)
                        && context
                            .runtime_filter_deployment
                            .as_ref()
                            .is_some_and(|claim| Arc::ptr_eq(&claim.completion, install_completion))
                });
            let removed = if just_created && matching_active {
                guard.active.remove(&query_id)
            } else {
                let context = if matching_active {
                    guard.active.get_mut(&query_id)
                } else if matching_second_chance {
                    guard.second_chance.get_mut(&query_id)
                } else {
                    None
                };
                if let Some(context) = context {
                    context.runtime_filter_deployment = None;
                    context.runtime_filter_service =
                        new_runtime_filter_service(query_id, &context.mem_tracker);
                }
                None
            };
            #[cfg(test)]
            let after_shutdown = guard.after_runtime_filter_service_shutdown.take();
            #[cfg(not(test))]
            let after_shutdown: Option<Arc<dyn Fn() + Send + Sync>> = None;
            (completion, removed, after_shutdown)
        };
        self.shutdown_and_publish_runtime_filter_deployment_aborted(
            query_id,
            epoch,
            Some(service.clone()),
            completion,
            after_shutdown,
            Some(install_completion.clone()),
        );
        drop(removed);
    }

    fn shutdown_and_publish_runtime_filter_deployment_aborted(
        &self,
        query_id: QueryId,
        epoch: DeploymentEpoch,
        service: Option<Arc<RuntimeFilterService>>,
        completion: Arc<RuntimeFilterDeploymentCompletion>,
        after_shutdown: Option<Arc<dyn Fn() + Send + Sync>>,
        install_completion: Option<Arc<RuntimeFilterDeploymentCompletion>>,
    ) {
        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if let Some(service) = service {
                service.shutdown_transport();
                if let Some(hook) = after_shutdown {
                    hook();
                }
            }
        }));
        self.publish_runtime_filter_deployment_aborted(query_id, epoch, &completion);
        if let Some(install_completion) = install_completion {
            install_completion.finish();
        }
        if let Err(payload) = unwind {
            std::panic::resume_unwind(payload);
        }
    }

    fn publish_runtime_filter_deployment_aborted(
        &self,
        query_id: QueryId,
        epoch: DeploymentEpoch,
        completion: &Arc<RuntimeFilterDeploymentCompletion>,
    ) {
        {
            let mut guard = self.inner.lock().expect("query_ctx_manager lock");
            let expires_at = runtime_filter_terminal_expiry(&guard);
            if let Some(record) = guard.runtime_filter_deployment_terminals.get_mut(&query_id)
                && record.epoch == epoch
                && matches!(
                    &record.state,
                    RuntimeFilterDeploymentTerminalState::Aborting(current)
                        if Arc::ptr_eq(current, completion)
                )
            {
                record.state = RuntimeFilterDeploymentTerminalState::Aborted;
                record.expires_at = expires_at;
            }
        }
        completion.finish();
    }

    pub(crate) fn abort_runtime_filter_deployment(
        &self,
        query_id: QueryId,
        epoch: DeploymentEpoch,
    ) -> Result<RuntimeFilterDeploymentAbortOutcome, RuntimeFilterDeploymentInstallError> {
        enum AbortStep {
            Wait {
                completion: Arc<RuntimeFilterDeploymentCompletion>,
                observer: Option<Arc<dyn Fn() + Send + Sync>>,
            },
            Shutdown {
                service: Option<Arc<RuntimeFilterService>>,
                removed: Option<QueryContext>,
                completion: Arc<RuntimeFilterDeploymentCompletion>,
                install_completion: Option<Arc<RuntimeFilterDeploymentCompletion>>,
                after_shutdown: Option<Arc<dyn Fn() + Send + Sync>>,
            },
        }

        loop {
            let step = {
                let mut guard = self.inner.lock().expect("query_ctx_manager lock");
                clean_expired_runtime_filter_terminals(&mut guard);
                if let Some(record) = guard.runtime_filter_deployment_terminals.get(&query_id) {
                    if record.epoch != epoch {
                        let kind = if epoch.get() < record.epoch.get() {
                            RuntimeFilterDeploymentInstallErrorKind::StaleEpoch
                        } else {
                            RuntimeFilterDeploymentInstallErrorKind::ConflictingDeployment
                        };
                        return Err(RuntimeFilterDeploymentInstallError::new(
                            kind,
                            "runtime filter abort epoch differs from the query terminal record",
                        ));
                    }
                    match &record.state {
                        RuntimeFilterDeploymentTerminalState::Aborted => {
                            return Ok(RuntimeFilterDeploymentAbortOutcome::Idempotent);
                        }
                        RuntimeFilterDeploymentTerminalState::Aborting(completion) => {
                            #[cfg(test)]
                            let observer = guard.abort_wait_observer.clone();
                            #[cfg(not(test))]
                            let observer: Option<
                                Arc<dyn Fn() + Send + Sync>,
                            > = None;
                            AbortStep::Wait {
                                completion: completion.clone(),
                                observer,
                            }
                        }
                    }
                } else {
                    let location = if guard.active.contains_key(&query_id) {
                        Some(true)
                    } else if guard.second_chance.contains_key(&query_id) {
                        Some(false)
                    } else {
                        None
                    };
                    let (service, install_completion) = if let Some(active) = location {
                        let context = if active {
                            guard.active.get(&query_id).expect("checked active")
                        } else {
                            guard
                                .second_chance
                                .get(&query_id)
                                .expect("checked second chance")
                        };
                        let install_completion = if let Some(claim) =
                            &context.runtime_filter_deployment
                        {
                            if epoch.get() < claim.install.epoch().get() {
                                return Err(RuntimeFilterDeploymentInstallError::new(
                                    RuntimeFilterDeploymentInstallErrorKind::StaleEpoch,
                                    "runtime filter abort epoch is older than the installed epoch",
                                ));
                            }
                            if epoch != claim.install.epoch() {
                                return Err(RuntimeFilterDeploymentInstallError::new(
                                    RuntimeFilterDeploymentInstallErrorKind::ConflictingDeployment,
                                    "runtime filter abort epoch differs from the installed epoch",
                                ));
                            }
                            Some(claim.completion.clone())
                        } else if context.num_fragments != 0 || context.num_active_fragments != 0 {
                            return Err(RuntimeFilterDeploymentInstallError::new(
                                RuntimeFilterDeploymentInstallErrorKind::ActiveFragments,
                                "runtime filter abort cannot target a query with active fragments and no matching deployment",
                            ));
                        } else {
                            None
                        };
                        (Some(context.runtime_filter_service()), install_completion)
                    } else {
                        (None, None)
                    };
                    ensure_runtime_filter_deployment_record_capacity(&guard, query_id)?;

                    let completion = Arc::new(RuntimeFilterDeploymentCompletion::new());
                    let expires_at = runtime_filter_terminal_expiry(&guard);
                    guard.runtime_filter_query_cancellations.remove(&query_id);
                    guard.runtime_filter_deployment_terminals.insert(
                        query_id,
                        RuntimeFilterDeploymentTerminalRecord {
                            epoch,
                            state: RuntimeFilterDeploymentTerminalState::Aborting(
                                completion.clone(),
                            ),
                            expires_at,
                        },
                    );
                    let removed = match location {
                        Some(true)
                            if guard.active.get(&query_id).is_some_and(|context| {
                                context.num_fragments == 0 && context.num_active_fragments == 0
                            }) =>
                        {
                            guard.active.remove(&query_id)
                        }
                        Some(false)
                            if guard.second_chance.get(&query_id).is_some_and(|context| {
                                context.num_fragments == 0 && context.num_active_fragments == 0
                            }) =>
                        {
                            guard.second_chance.remove(&query_id)
                        }
                        _ => None,
                    };
                    #[cfg(test)]
                    let after_shutdown = guard.after_runtime_filter_service_shutdown.take();
                    #[cfg(not(test))]
                    let after_shutdown: Option<Arc<dyn Fn() + Send + Sync>> = None;
                    AbortStep::Shutdown {
                        service,
                        removed,
                        completion,
                        install_completion,
                        after_shutdown,
                    }
                }
            };

            match step {
                AbortStep::Wait {
                    completion,
                    observer,
                } => {
                    if let Some(observer) = observer {
                        observer();
                    }
                    completion.wait();
                }
                AbortStep::Shutdown {
                    service,
                    removed,
                    completion,
                    install_completion,
                    after_shutdown,
                } => {
                    self.shutdown_and_publish_runtime_filter_deployment_aborted(
                        query_id,
                        epoch,
                        service,
                        completion,
                        after_shutdown,
                        install_completion,
                    );
                    drop(removed);
                    return Ok(RuntimeFilterDeploymentAbortOutcome::Applied);
                }
            }
        }
    }

    #[cfg(feature = "compat")]
    pub(crate) fn get_or_register_compat(
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
            QueryContextGeneration::StarRocksUnbound,
            false,
        )
    }

    #[cfg(feature = "compat")]
    pub(crate) fn ensure_compat_context(
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
            QueryContextGeneration::StarRocksUnbound,
            false,
        )
    }

    #[cfg(feature = "compat")]
    pub(crate) fn prepare_starrocks_admission(
        &self,
        query_id: QueryId,
        delivery_expire: Duration,
        query_expire: Duration,
        cache_options: CacheOptions,
    ) -> Result<Arc<MemTracker>, String> {
        self.ensure_compat_context(query_id, false, delivery_expire, query_expire)?;
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        let context = if guard.active.contains_key(&query_id) {
            guard.active.get_mut(&query_id).expect("checked active")
        } else {
            guard
                .second_chance
                .get_mut(&query_id)
                .ok_or_else(|| "QueryContext disappeared during StarRocks admission".to_string())?
        };
        context.set_cache_options(cache_options)?;
        context.starrocks_preparing_admissions += 1;
        Ok(context.mem_tracker())
    }

    #[cfg(feature = "compat")]
    pub(crate) fn release_starrocks_admission(&self, query_id: QueryId) {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        let location = if guard.active.contains_key(&query_id) {
            Some(true)
        } else if guard.second_chance.contains_key(&query_id) {
            Some(false)
        } else {
            None
        };
        let Some(in_active) = location else {
            return;
        };
        let context = if in_active {
            guard.active.get_mut(&query_id).expect("checked active")
        } else {
            guard
                .second_chance
                .get_mut(&query_id)
                .expect("checked second chance")
        };
        context.starrocks_preparing_admissions =
            context.starrocks_preparing_admissions.saturating_sub(1);
        let remove = context.starrocks_preparing_admissions == 0
            && context.execution_generation == QueryContextGeneration::StarRocksUnbound
            && context.num_fragments == 0;
        if remove {
            if in_active {
                guard.active.remove(&query_id);
            } else {
                guard.second_chance.remove(&query_id);
            }
        }
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
            QueryContextGeneration::Native,
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
        native_runtime_filter_lifecycle: bool,
    ) -> Result<(), String> {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        clean_expired_runtime_filter_terminals(&mut guard);
        if native_runtime_filter_lifecycle {
            if let Some(error) = guard
                .runtime_filter_query_cancellation_errors
                .get(&query_id)
            {
                return Err(error.clone());
            }
            if guard
                .runtime_filter_deployment_terminals
                .contains_key(&query_id)
                || guard
                    .runtime_filter_query_cancellations
                    .contains_key(&query_id)
            {
                return Err("runtime filter deployment query was already cancelled".to_string());
            }
        }
        if native_runtime_filter_lifecycle
            && guard
                .active
                .get(&query_id)
                .or_else(|| guard.second_chance.get(&query_id))
                .is_some_and(|context| context.cancelled_by_fe)
        {
            return Err("runtime filter deployment query was already cancelled".to_string());
        }
        if native_runtime_filter_lifecycle
            && guard
                .active
                .get(&query_id)
                .or_else(|| guard.second_chance.get(&query_id))
                .and_then(|context| context.runtime_filter_deployment.as_ref())
                .is_some_and(|claim| claim.state == RuntimeFilterDeploymentClaimState::Installing)
        {
            return Err(
                "runtime filter deployment is still installing; fragment registration is forbidden"
                    .to_string(),
            );
        }
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

    #[cfg(all(test, feature = "compat"))]
    pub(crate) fn get_or_register_compat_generation(
        &self,
        execution: QueryExecutionKey,
        return_error_if_not_exist: bool,
        delivery_expire: Duration,
        query_expire: Duration,
    ) -> Result<(), String> {
        let generation = execution
            .starrocks_generation()
            .ok_or_else(|| "compat query registration requires StarRocks generation".to_string())?;
        let query_id = execution.query_id();
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        if guard
            .finst_to_query
            .values()
            .any(|existing| existing.query_id() == query_id && *existing != execution)
        {
            return Err(format!(
                "previous StarRocks query generation still has fragment routing: query_id={query_id}"
            ));
        }
        if let Some(ctx) = guard.active.get_mut(&query_id) {
            ctx.bind_starrocks_generation(generation)?;
            ctx.increment_num_fragments();
            return Ok(());
        }
        if guard.second_chance.contains_key(&query_id) {
            guard
                .second_chance
                .get_mut(&query_id)
                .expect("checked")
                .bind_starrocks_generation(generation)?;
            let mut ctx = guard.second_chance.remove(&query_id).expect("checked");
            ctx.increment_num_fragments();
            guard.active.insert(query_id, ctx);
            return Ok(());
        }
        if return_error_if_not_exist {
            return Err("Query terminates prematurely (missing QueryContext)".to_string());
        }
        let mut ctx = QueryContext::new_with_generation(
            query_id,
            QueryContextGeneration::StarRocks(generation),
            delivery_expire,
            query_expire,
        );
        ctx.increment_num_fragments();
        guard.active.insert(query_id, ctx);
        Ok(())
    }

    #[cfg(feature = "compat")]
    pub(crate) fn commit_starrocks_handoff<F>(
        &self,
        handoff: StarRocksQueryHandoff,
        make_cleanup_lease: F,
    ) -> Result<Arc<MemTracker>, String>
    where
        F: FnOnce() -> Option<QueryCleanupLease>,
    {
        let generation = handoff
            .execution
            .starrocks_generation()
            .ok_or_else(|| "StarRocks handoff requires query generation".to_string())?;
        if handoff.fragment_count == 0 || handoff.fragment_count != handoff.instances.len() {
            return Err("StarRocks handoff fragment count does not match instances".to_string());
        }

        let query_id = handoff.execution.query_id();
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        if guard
            .finst_to_query
            .values()
            .any(|existing| existing.query_id() == query_id && *existing != handoff.execution)
        {
            return Err(format!(
                "previous StarRocks query generation still has fragment routing: query_id={query_id}"
            ));
        }
        let mut incoming_finsts = HashSet::with_capacity(handoff.instances.len());
        for (finst_id, _) in &handoff.instances {
            if !incoming_finsts.insert(*finst_id) {
                return Err(format!(
                    "duplicate fragment instance in StarRocks handoff: finst_id={finst_id}"
                ));
            }
            if let Some(existing) = guard.finst_to_query.get(finst_id) {
                return Err(format!(
                    "fragment instance is already registered: finst_id={finst_id} execution={existing:?}"
                ));
            }
        }

        let existing = guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id));
        if let Some(context) = existing {
            match context.execution_generation {
                QueryContextGeneration::StarRocksUnbound if context.num_fragments == 0 => {}
                QueryContextGeneration::StarRocks(current) if current == generation => {}
                QueryContextGeneration::StarRocksUnbound => {
                    return Err(
                        "cannot bind StarRocks generation after fragment registration".to_string(),
                    );
                }
                QueryContextGeneration::StarRocks(current) => {
                    return Err(format!(
                        "StarRocks query generation mismatch: current={} requested={}",
                        current.get(),
                        generation.get()
                    ));
                }
                QueryContextGeneration::Native => {
                    return Err(
                        "cannot bind StarRocks generation to native query context".to_string()
                    );
                }
            }
            if context
                .cache_options
                .as_ref()
                .is_some_and(|current| current != &handoff.cache_options)
            {
                return Err("cache options mismatch for query".to_string());
            }
            context.validate_row_pos_descs(&handoff.row_pos_descs)?;
        }

        // All fallible checks finish before the descriptor lease is created or any Q state is
        // published. The caller may therefore keep the descriptor-cache lock across this call
        // without risking a cleanup-lease drop while that lock is held.
        let cleanup_lease = make_cleanup_lease();
        let mut context = if let Some(context) = guard.active.remove(&query_id) {
            context
        } else if let Some(context) = guard.second_chance.remove(&query_id) {
            context
        } else {
            QueryContext::new_with_generation(
                query_id,
                QueryContextGeneration::StarRocks(generation),
                handoff.delivery_expire,
                handoff.query_expire,
            )
        };
        if context.execution_generation == QueryContextGeneration::StarRocksUnbound {
            context.execution_generation = QueryContextGeneration::StarRocks(generation);
        }
        if context.cache_options.is_none() {
            context.cache_options = Some(handoff.cache_options);
        }
        if let Some(snapshot) = handoff.descriptor_snapshot {
            context.desc_snapshot = Some(snapshot);
        }
        if let Some(total_fragments) = handoff.total_fragments {
            context.total_fragments = Some(
                context
                    .total_fragments
                    .map_or(total_fragments, |current| current.max(total_fragments)),
            );
        }
        for (tuple_id, descriptor) in handoff.row_pos_descs {
            context.row_pos_descs.entry(tuple_id).or_insert(descriptor);
        }
        context.register_lookup_fetchers(&handoff.lookup_fetchers);
        context.num_fragments += handoff.fragment_count;
        context.num_active_fragments += handoff.fragment_count;
        if let Some(lease) = cleanup_lease {
            context.attach_cleanup_lease(lease);
        }
        let mem_tracker = context.mem_tracker();
        guard.active.insert(query_id, context);
        for (finst_id, contracts) in handoff.instances {
            guard.finst_to_query.insert(finst_id, handoff.execution);
            guard
                .incremental_change_op_slots
                .insert(finst_id, contracts);
        }
        Ok(mem_tracker)
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

    pub(crate) fn runtime_filter_service_for_ingress(
        &self,
        query_id: QueryId,
    ) -> Option<Arc<RuntimeFilterService>> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        if guard
            .runtime_filter_deployment_terminals
            .contains_key(&query_id)
        {
            return None;
        }
        if let Some(context) = guard.active.get(&query_id) {
            if !context.has_no_active_instances() || !context.is_query_expired() {
                return Some(context.runtime_filter_service());
            }
            return None;
        }
        guard.second_chance.get(&query_id).and_then(|context| {
            (!context.is_delivery_expired()).then(|| context.runtime_filter_service())
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
        query_ids.sort_by_key(|query_id| (query_id.hi, query_id.lo));
        query_ids.dedup();
        query_ids
    }

    pub(crate) fn runtime_filter_context_for_native_execution(
        &self,
        query_id: QueryId,
        fragment_instance_id: UniqueId,
    ) -> Result<NativeRuntimeFilterExecutionContext, String> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        if guard
            .runtime_filter_deployment_terminals
            .contains_key(&query_id)
            || guard
                .runtime_filter_query_cancellations
                .contains_key(&query_id)
        {
            return Err("runtime filter deployment query was cancelled".to_string());
        }
        let context = guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .ok_or_else(|| "runtime filter native execution QueryContext not found".to_string())?;
        if context.cancelled_by_fe {
            return Err("runtime filter deployment query was cancelled".to_string());
        }
        let claim = context.runtime_filter_deployment.as_ref().ok_or_else(|| {
            "runtime filter native execution requires deployment state Installed".to_string()
        })?;
        if claim.state != RuntimeFilterDeploymentClaimState::Installed {
            return Err(format!(
                "runtime filter native execution requires deployment state Installed, current={:?}",
                claim.state
            ));
        }
        Ok(NativeRuntimeFilterExecutionContext::new(
            context.runtime_filter_service(),
            UniqueId {
                hi: query_id.hi,
                lo: query_id.lo,
            },
            claim.install.epoch(),
            fragment_instance_id,
        ))
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
                if ctx.is_dead() && !ctx.runtime_filter_deployment_is_installing() {
                    guard.second_chance.remove(&query_id)
                } else {
                    None
                }
            } else {
                return Err(format!("QueryContext not found: query_id={query_id}"));
            }
        };
        if let Some(ctx) = removed {
            ctx.runtime_filter_service().shutdown();
            drop(ctx);
            self.remove_runtime_filter_lifecycle_if_context_absent(query_id);
        }
        Ok(())
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

    #[cfg(feature = "compat")]
    pub(crate) fn register_lake_glm(
        &self,
        query_id: QueryId,
        row_source_slot: SlotId,
        info: LakeGlmScanInfo,
    ) -> Result<(), String> {
        self.with_context_mut(query_id, |ctx| {
            ctx.register_lake_glm(row_source_slot, info);
            Ok(())
        })
    }

    #[cfg(feature = "compat")]
    pub(crate) fn lake_glm_info(
        &self,
        query_id: QueryId,
        row_source_slot: SlotId,
    ) -> Option<LakeGlmScanInfo> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .and_then(|ctx| ctx.lake_glm_info(row_source_slot))
            .cloned()
    }

    pub(crate) fn query_mem_tracker(&self, query_id: QueryId) -> Option<Arc<MemTracker>> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .map(|ctx| ctx.mem_tracker())
    }

    pub(crate) fn descriptor_snapshot(&self, query_id: QueryId) -> Option<Arc<DescriptorSnapshot>> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .and_then(|ctx| ctx.desc_snapshot.clone())
    }

    #[cfg(feature = "compat")]
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

    #[cfg(feature = "compat")]
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

    #[cfg(feature = "compat")]
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

    #[cfg(all(test, feature = "compat"))]
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

    #[cfg(all(test, feature = "compat"))]
    pub(crate) fn register_starrocks_finsts<I>(
        &self,
        finst_ids: I,
        execution: QueryExecutionKey,
    ) -> Result<(), String>
    where
        I: IntoIterator<Item = UniqueId>,
    {
        if execution.starrocks_generation().is_none() {
            return Err("StarRocks finst registration requires generation".to_string());
        }
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        let context = guard
            .active
            .get(&execution.query_id())
            .ok_or_else(|| "QueryContext not found".to_string())?;
        if !context.matches_execution(execution) {
            return Err("StarRocks query generation is not active".to_string());
        }
        for finst_id in finst_ids {
            guard.finst_to_query.insert(finst_id, execution);
        }
        Ok(())
    }

    #[cfg(all(test, feature = "compat"))]
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

    pub(crate) fn query_id_by_finst(&self, finst_id: UniqueId) -> Option<QueryId> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .finst_to_query
            .get(&finst_id)
            .map(|execution| execution.query_id())
    }

    #[cfg(all(test, feature = "compat"))]
    pub(crate) fn query_execution_by_finst(&self, finst_id: UniqueId) -> Option<QueryExecutionKey> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard.finst_to_query.get(&finst_id).copied()
    }

    pub(crate) fn unregister_finst(&self, finst_id: UniqueId) {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        guard.finst_to_query.remove(&finst_id);
        #[cfg(feature = "compat")]
        guard.incremental_scan_nodes.remove(&finst_id);
        #[cfg(feature = "compat")]
        guard.pending_incremental_scan_ranges.remove(&finst_id);
        #[cfg(feature = "compat")]
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
                context.num_fragments == 0
                    && context.num_active_fragments == 0
                    && context.runtime_filter_deployment.is_none()
            };
            remove_empty_context.then(|| {
                guard
                    .active
                    .remove(&query_id)
                    .expect("checked empty active context")
            })
        };
        if let Some(context) = removed {
            context.runtime_filter_service().shutdown();
            drop(context);
            self.remove_runtime_filter_lifecycle_if_context_absent(query_id);
        }
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
        #[cfg(feature = "compat")]
        guard.incremental_scan_nodes.remove(&finst_id);
        #[cfg(feature = "compat")]
        guard.pending_incremental_scan_ranges.remove(&finst_id);
        #[cfg(feature = "compat")]
        guard.incremental_change_op_slots.remove(&finst_id);
    }

    /// Undo a StarRocks batch handoff before its shared start gate is released.
    ///
    /// The handoff publishes every fragment route and the query context atomically.
    /// Its adapter-side rollback therefore removes the same complete route set and
    /// drops the context, including descriptor cleanup leases, as one operation.
    #[cfg(feature = "compat")]
    pub(crate) fn rollback_starrocks_handoff(
        &self,
        execution: QueryExecutionKey,
        fragment_instance_ids: &[UniqueId],
    ) -> bool {
        let query_id = execution.query_id();
        let removed = {
            let mut guard = self.inner.lock().expect("query_ctx_manager lock");
            let context_matches = guard
                .active
                .get(&query_id)
                .is_some_and(|context| context.matches_execution(execution));
            let registered = guard
                .finst_to_query
                .iter()
                .filter_map(|(finst_id, current)| (*current == execution).then_some(*finst_id))
                .collect::<HashSet<_>>();
            let requested = fragment_instance_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>();
            if !context_matches || requested != registered {
                return false;
            }

            for finst_id in fragment_instance_ids.iter().rev() {
                guard.finst_to_query.remove(finst_id);
                guard.incremental_scan_nodes.remove(finst_id);
                guard.pending_incremental_scan_ranges.remove(finst_id);
                guard.incremental_change_op_slots.remove(finst_id);
            }
            guard.active.remove(&query_id)
        };

        if let Some(context) = removed {
            context.runtime_filter_service().shutdown();
            drop(context);
            self.remove_runtime_filter_lifecycle_if_context_absent(query_id);
            true
        } else {
            false
        }
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

    pub(crate) fn is_query_canceled(&self, query_id: QueryId) -> bool {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .runtime_filter_query_cancellations
            .contains_key(&query_id)
            || guard
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
        cancellation_error: Option<&str>,
    ) -> RuntimeFilterQueryCancellationAction {
        clean_expired_runtime_filter_terminals(inner);
        let location = if inner.active.contains_key(&query_id) {
            Some(true)
        } else if inner.second_chance.contains_key(&query_id) {
            Some(false)
        } else {
            None
        };
        let Some(active) = location else {
            return RuntimeFilterQueryCancellationAction::default();
        };
        let matches = {
            let context = if active {
                inner.active.get(&query_id).expect("checked active")
            } else {
                inner
                    .second_chance
                    .get(&query_id)
                    .expect("checked second chance")
            };
            expected_execution.is_none_or(|execution| context.matches_execution(execution))
        };
        if !matches {
            return RuntimeFilterQueryCancellationAction::default();
        }
        if let Some(error) = cancellation_error {
            inner
                .runtime_filter_query_cancellation_errors
                .entry(query_id)
                .or_insert_with(|| error.to_string());
        }

        let expires_at = runtime_filter_terminal_expiry(inner);
        let (service, claim, retain_epochless_cancellation) = {
            let context = if active {
                inner.active.get_mut(&query_id).expect("checked active")
            } else {
                inner
                    .second_chance
                    .get_mut(&query_id)
                    .expect("checked second chance")
            };
            context.cancelled_by_fe = true;
            let claim = context
                .runtime_filter_deployment
                .as_ref()
                .map(|claim| (claim.install.epoch(), claim.completion.clone()));
            let retain_epochless_cancellation = claim.is_none()
                && matches!(context.execution_generation, QueryContextGeneration::Native);
            if retain_epochless_cancellation {
                context.runtime_filter_cancel_retain_until = Some(expires_at);
            }
            (
                context.runtime_filter_service(),
                claim,
                retain_epochless_cancellation,
            )
        };

        if inner
            .runtime_filter_deployment_terminals
            .contains_key(&query_id)
        {
            return RuntimeFilterQueryCancellationAction {
                service: Some(service),
                terminalization: None,
            };
        }

        let Some((epoch, install_completion)) = claim else {
            if retain_epochless_cancellation {
                if let Some(record) = inner.runtime_filter_query_cancellations.get_mut(&query_id) {
                    record.expires_at = expires_at;
                } else if runtime_filter_deployment_record_count(inner)
                    < runtime_filter_terminal_capacity(inner)
                {
                    inner.runtime_filter_query_cancellations.insert(
                        query_id,
                        RuntimeFilterQueryCancellationRecord { expires_at },
                    );
                }
                // Capacity exhaustion cannot be returned through ordinary cancellation.
                // The target context remains retained and cancelled instead; never evict a
                // live control record or block unrelated query ids.
            }
            return RuntimeFilterQueryCancellationAction {
                service: Some(service),
                terminalization: None,
            };
        };

        let completion = Arc::new(RuntimeFilterDeploymentCompletion::new());
        inner.runtime_filter_query_cancellations.remove(&query_id);
        inner.runtime_filter_deployment_terminals.insert(
            query_id,
            RuntimeFilterDeploymentTerminalRecord {
                epoch,
                state: RuntimeFilterDeploymentTerminalState::Aborting(completion.clone()),
                expires_at,
            },
        );
        #[cfg(test)]
        let after_shutdown = inner.after_runtime_filter_service_shutdown.take();
        #[cfg(not(test))]
        let after_shutdown: Option<Arc<dyn Fn() + Send + Sync>> = None;
        RuntimeFilterQueryCancellationAction {
            service: Some(service),
            terminalization: Some(RuntimeFilterQueryCancellationTerminalization {
                epoch,
                completion,
                install_completion,
                after_shutdown,
            }),
        }
    }

    fn execute_runtime_filter_query_cancellation(
        &self,
        query_id: QueryId,
        action: RuntimeFilterQueryCancellationAction,
    ) -> std::thread::Result<()> {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if let Some(terminalization) = action.terminalization {
                self.shutdown_and_publish_runtime_filter_deployment_aborted(
                    query_id,
                    terminalization.epoch,
                    action.service,
                    terminalization.completion,
                    terminalization.after_shutdown,
                    Some(terminalization.install_completion),
                );
            } else if let Some(service) = action.service {
                service.cancel();
            }
        }))
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
        let (cancellation, finsts) = {
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
            (cancellation, finsts)
        };
        let cancellation_unwind =
            self.execute_runtime_filter_query_cancellation(execution.query_id(), cancellation);
        if let Err(payload) = cancellation_unwind {
            std::panic::resume_unwind(payload);
        }
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

    #[cfg(all(test, feature = "compat"))]
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
                    crate::runtime::exchange::cancel_fragment(id.hi, id.lo);
                }
                finsts
            }
            None => {
                crate::runtime::exchange::cancel_fragment(finst_id.hi, finst_id.lo);
                vec![finst_id]
            }
        }
    }

    pub(crate) fn finish_fragment(&self, query_id: QueryId) {
        let decision = self.finish_fragment_internal(query_id);
        if decision.remove_runtime_filter_lifecycle_after_report {
            self.remove_runtime_filter_lifecycle_if_context_absent(query_id);
        }
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
        if decision.remove_runtime_filter_lifecycle_after_report {
            self.remove_runtime_filter_lifecycle_if_context_absent(execution.query_id());
        }
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
        if decision.remove_runtime_filter_lifecycle_after_report {
            self.remove_runtime_filter_lifecycle_if_context_absent(query_id);
        }
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
                remove_runtime_filter_lifecycle_after_report: false,
            };
        }
        if guard
            .active
            .get(&query_id)
            .is_some_and(QueryContext::runtime_filter_deployment_is_installing)
        {
            return FragmentFinishReportDecision::default();
        }
        let Some(mut ctx) = guard.active.remove(&query_id) else {
            return FragmentFinishReportDecision {
                include_runtime_filter_profile: true,
                remove_runtime_filter_lifecycle_after_report: false,
            };
        };
        let no_active_fragments = ctx.count_down_fragments();
        if !no_active_fragments {
            guard.active.insert(query_id, ctx);
            return FragmentFinishReportDecision::default();
        }
        if ctx.runtime_filter_cancel_is_retained(runtime_filter_terminal_now(&guard)) {
            ctx.extend_delivery_lifetime();
            guard.second_chance.insert(query_id, ctx);
            return FragmentFinishReportDecision {
                include_runtime_filter_profile: true,
                remove_runtime_filter_lifecycle_after_report: false,
            };
        }
        if ctx.is_dead() {
            let decision = FragmentFinishReportDecision {
                include_runtime_filter_profile: true,
                remove_runtime_filter_lifecycle_after_report: true,
            };
            drop(guard);
            ctx.runtime_filter_service().shutdown();
            drop(ctx);
            return decision;
        }
        ctx.extend_delivery_lifetime();
        guard.second_chance.insert(query_id, ctx);
        FragmentFinishReportDecision {
            include_runtime_filter_profile: true,
            remove_runtime_filter_lifecycle_after_report: false,
        }
    }
}

fn remove_runtime_filter_lifecycle(query_id: QueryId) {
    RuntimeFilterLifecycleRegistry::global()
        .remove_query(QueryKey::from_hi_lo(query_id.hi, query_id.lo));
}

fn map_runtime_filter_install_error(
    kind: InstallContractErrorKind,
    detail: String,
) -> RuntimeFilterDeploymentInstallError {
    let kind = match kind {
        InstallContractErrorKind::EpochMismatch => {
            RuntimeFilterDeploymentInstallErrorKind::StaleEpoch
        }
        InstallContractErrorKind::ConflictingDeployment => {
            RuntimeFilterDeploymentInstallErrorKind::ConflictingDeployment
        }
        InstallContractErrorKind::ServiceClosed => {
            RuntimeFilterDeploymentInstallErrorKind::QueryAborted
        }
        _ => RuntimeFilterDeploymentInstallErrorKind::Internal,
    };
    RuntimeFilterDeploymentInstallError::new(kind, detail)
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
        let finst_id = UniqueId {
            hi: 86_103,
            lo: 86_104,
        };
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

#[cfg(all(test, feature = "compat"))]
mod generation_race_tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex, mpsc};
    use std::time::Duration;

    use crate::common::types::UniqueId;

    use super::{
        QueryContextManager, QueryContextManagerInner, QueryExecutionKey, QueryId,
        StarRocksQueryGeneration,
    };

    fn test_manager() -> Arc<QueryContextManager> {
        Arc::new(QueryContextManager {
            inner: Mutex::new(QueryContextManagerInner::default()),
            stopped: AtomicBool::new(false),
        })
    }

    #[test]
    fn cancel_resolution_is_atomic_with_old_generation_drain_and_reuse() {
        let manager = test_manager();
        let query_id = QueryId {
            hi: 86_001,
            lo: 86_002,
        };
        let old_finst = UniqueId {
            hi: 86_003,
            lo: 86_004,
        };
        let new_finst = UniqueId {
            hi: 86_005,
            lo: 86_006,
        };
        let old_execution = QueryExecutionKey::starrocks(
            query_id,
            StarRocksQueryGeneration::new(1).expect("old generation"),
        );
        let new_execution = QueryExecutionKey::starrocks(
            query_id,
            StarRocksQueryGeneration::new(2).expect("new generation"),
        );
        manager
            .get_or_register_compat_generation(
                old_execution,
                false,
                Duration::from_secs(60),
                Duration::from_secs(60),
            )
            .expect("old context");
        manager
            .register_starrocks_finsts([old_finst], old_execution)
            .expect("old routing");

        let (resolved_tx, resolved_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let cancel_manager = Arc::clone(&manager);
        let cancel = std::thread::spawn(move || {
            cancel_manager.cancel_finst_with_binding_observer(
                old_finst,
                "old generation cancel".to_string(),
                || {
                    resolved_tx.send(()).expect("signal resolved binding");
                    release_rx.recv().expect("release cancel lock");
                },
            )
        });
        resolved_rx.recv().expect("cancel resolved old binding");

        let (reuse_started_tx, reuse_started_rx) = mpsc::channel();
        let reuse_manager = Arc::clone(&manager);
        let reuse = std::thread::spawn(move || {
            reuse_started_tx.send(()).expect("signal reuse attempt");
            reuse_manager.unregister_finst_execution(old_finst, old_execution);
            reuse_manager.finish_fragment_execution(old_execution);
            reuse_manager
                .get_or_register_compat_generation(
                    new_execution,
                    false,
                    Duration::from_secs(60),
                    Duration::from_secs(60),
                )
                .expect("new context after old routing drain");
            reuse_manager
                .register_starrocks_finsts([new_finst], new_execution)
                .expect("new routing");
        });
        reuse_started_rx.recv().expect("reuse thread started");
        release_tx.send(()).expect("release cancel lock");

        let cancelled = cancel.join().expect("cancel thread");
        reuse.join().expect("reuse thread");
        assert_eq!(cancelled.query_id, Some(query_id));
        assert_eq!(cancelled.finsts, vec![old_finst]);
        assert_eq!(
            manager.query_execution_by_finst(new_finst),
            Some(new_execution)
        );
        assert!(
            !manager.is_query_canceled(query_id),
            "old cancellation must only touch the service captured for its generation"
        );

        manager.unregister_finst_execution(new_finst, new_execution);
        manager.cancel_query_execution(new_execution, "test cleanup".to_string());
        manager.finish_fragment_execution(new_execution);
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
        let query_id = QueryId { hi: 901, lo: 902 };
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
        let query_id = QueryId { hi: 911, lo: 912 };
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
        let query_id = QueryId { hi: 921, lo: 922 };
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
        let qid = QueryId { hi: 11, lo: 22 };
        let finst_a = UniqueId { hi: 101, lo: 201 };
        let finst_b = UniqueId { hi: 102, lo: 202 };
        let key_a = ExchangeKey {
            finst_id_hi: finst_a.hi,
            finst_id_lo: finst_a.lo,
            node_id: 301,
        };
        let key_b = ExchangeKey {
            finst_id_hi: finst_b.hi,
            finst_id_lo: finst_b.lo,
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
        finsts.sort_by_key(|id| (id.hi, id.lo));

        assert_eq!(finsts, vec![finst_a, finst_b]);
        assert!(mgr.is_query_canceled(qid));
        assert!(snapshot_receiver_state(key_a).is_none());
        assert!(snapshot_receiver_state(key_b).is_none());
    }

    #[test]
    fn unmapped_finst_cancels_its_own_receiver_only() {
        let mgr = test_manager();
        let finst = UniqueId { hi: 201, lo: 202 };
        let key = ExchangeKey {
            finst_id_hi: finst.hi,
            finst_id_lo: finst.lo,
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
        let query_id = QueryId {
            hi: 4_101,
            lo: 4_102,
        };
        let query_key = QueryKey::from_hi_lo(query_id.hi, query_id.lo);
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
        let query_id = QueryId {
            hi: 4_131,
            lo: 4_132,
        };
        let first = UniqueId { hi: 4_133, lo: 1 };
        let second = UniqueId { hi: 4_133, lo: 2 };

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
            .remove_query(QueryKey::from_hi_lo(query_id.hi, query_id.lo));
    }

    #[test]
    fn finish_fragment_for_report_claims_runtime_filter_export_once_before_cleanup() {
        let mgr = test_manager();
        let query_id = QueryId {
            hi: 4_151,
            lo: 4_152,
        };
        let query_key = QueryKey::from_hi_lo(query_id.hi, query_id.lo);
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
        assert!(second.remove_runtime_filter_lifecycle_after_report);
        assert!(registry.snapshot(query_key).is_some());

        mgr.cleanup_after_fragment_report(query_id, second);

        assert!(registry.snapshot(query_key).is_none());
    }

    #[test]
    fn report_cleanup_preserves_lifecycle_for_recreated_context() {
        let mgr = test_manager();
        let query_id = QueryId {
            hi: 4_181,
            lo: 4_182,
        };
        let query_key = QueryKey::from_hi_lo(query_id.hi, query_id.lo);
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
        assert!(decision.remove_runtime_filter_lifecycle_after_report);
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
        let query_id = QueryId {
            hi: 4_201,
            lo: 4_202,
        };
        let query_key = QueryKey::from_hi_lo(query_id.hi, query_id.lo);
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

#[cfg(all(test, feature = "compat"))]
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
        let finst_id = UniqueId { hi: 91, lo: 92 };
        manager.register_finsts_with_incremental_contracts(
            [(finst_id, HashMap::from([(41, Some(SlotId::new(7)))]))],
            QueryId { hi: 81, lo: 82 },
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
        let finst_id = UniqueId { hi: 93, lo: 94 };
        manager.register_finsts_with_incremental_contracts(
            [(finst_id, HashMap::from([(41, None)]))],
            QueryId { hi: 83, lo: 84 },
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
mod row_position_descriptor_tests {
    use std::collections::HashMap;

    use super::{QueryContext, QueryId};
    use crate::common::ids::SlotId;
    use crate::exec::row_position::{RowPositionDescriptor, RowPositionType};

    fn descriptor(row_source_slot: u32) -> RowPositionDescriptor {
        RowPositionDescriptor {
            row_position_type: RowPositionType::Lake,
            row_source_slot: SlotId::new(row_source_slot),
            fetch_ref_slots: vec![SlotId::new(12), SlotId::new(13), SlotId::new(14)],
            lookup_ref_slots: vec![SlotId::new(15)],
        }
    }

    #[test]
    fn row_position_registration_is_idempotent_and_merges_distinct_tuples() {
        let mut context = QueryContext::new(
            QueryId { hi: 71, lo: 72 },
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(1),
        );
        context
            .merge_row_pos_descs(HashMap::from([(3, descriptor(11))]))
            .expect("register lookup descriptor");
        context
            .merge_row_pos_descs(HashMap::from([(3, descriptor(11)), (4, descriptor(21))]))
            .expect("idempotent registration and distinct tuple merge");

        assert_eq!(
            context.row_pos_desc(3).unwrap().row_source_slot,
            SlotId::new(11)
        );
        assert_eq!(
            context.row_pos_desc(4).unwrap().row_source_slot,
            SlotId::new(21)
        );
    }

    #[test]
    fn row_position_registration_rejects_conflicting_tuple_metadata() {
        let mut context = QueryContext::new(
            QueryId { hi: 73, lo: 74 },
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(1),
        );
        context
            .merge_row_pos_descs(HashMap::from([(3, descriptor(11))]))
            .expect("register lookup descriptor");

        let error = context
            .merge_row_pos_descs(HashMap::from([(3, descriptor(21)), (4, descriptor(31))]))
            .expect_err("conflicting metadata must fail");
        assert_eq!(error, "conflicting row position descriptor for tuple_id=3");
        assert!(
            context.row_pos_desc(4).is_none(),
            "a rejected registration must not leave partial metadata"
        );
    }
}

#[cfg(test)]
pub(crate) mod runtime_filter_service_lifecycle_tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Barrier, Mutex, Weak, mpsc};
    use std::time::{Duration, Instant};

    use arrow::datatypes::DataType;

    use super::{QueryContext, QueryContextManager, QueryContextManagerInner, QueryId};
    use crate::common::types::UniqueId;
    use crate::runtime::runtime_filter_observability::{QueryKey, RuntimeFilterLifecycleRegistry};
    use crate::runtime_filter::model::contract::{
        ArtifactCapability, BindingId, ChannelId, CompletionFenceKind, CompletionRequirement,
        ConsumerActivation, ContributionKind, CoverageWitnessId, NullSemantics,
        ReductionRequirement, RuntimeFilterLifecycle, RuntimeFilterLogicalDomain,
        RuntimeFilterPolicyRequirement,
    };
    use crate::runtime_filter::model::coverage::Coverage;
    use crate::runtime_filter::port::events::{RuntimeFilterEvent, RuntimeFilterEventSink};
    use crate::runtime_filter::port::identity::{
        DeploymentEpoch, RouteEdgeId, RuntimeFilterParticipantId,
    };
    use crate::runtime_filter::port::install::{
        ConsumerDeployment, ProducerDeployment, RuntimeFilterChannelDeployment,
        RuntimeFilterCoreBudget, RuntimeFilterInstallView, RuntimeFilterParticipantInstall,
        local_participant_install_for_test,
    };
    use crate::runtime_filter::port::producer::{InstallContractErrorKind, InstallOutcome};
    use crate::runtime_filter::service::RuntimeFilterService;

    fn test_manager() -> Arc<QueryContextManager> {
        Arc::new(QueryContextManager {
            inner: Mutex::new(QueryContextManagerInner::default()),
            stopped: AtomicBool::new(false),
        })
    }

    pub(super) fn query_id(lo: i64) -> QueryId {
        QueryId { hi: 70, lo }
    }

    fn uid(lo: i64) -> UniqueId {
        UniqueId { hi: 70, lo }
    }

    pub(crate) fn participant_install() -> RuntimeFilterParticipantInstall {
        participant_install_with_consumer_and_producer_instances(
            ConsumerActivation::BlockingSnapshot,
            BTreeSet::from([ArtifactCapability::Membership]),
            BTreeSet::from([uid(30)]),
        )
    }

    pub(crate) fn participant_install_with_expected_producer_instances(
        expected_fragment_instances: BTreeSet<UniqueId>,
    ) -> RuntimeFilterParticipantInstall {
        participant_install_with_consumer_and_producer_instances(
            ConsumerActivation::BlockingSnapshot,
            BTreeSet::from([ArtifactCapability::Membership]),
            expected_fragment_instances,
        )
    }

    pub(crate) fn participant_install_with_consumer(
        activation: ConsumerActivation,
        capabilities: BTreeSet<ArtifactCapability>,
    ) -> RuntimeFilterParticipantInstall {
        participant_install_with_consumer_and_producer_instances(
            activation,
            capabilities,
            BTreeSet::from([uid(30)]),
        )
    }

    fn participant_install_with_consumer_and_producer_instances(
        activation: ConsumerActivation,
        capabilities: BTreeSet<ArtifactCapability>,
        expected_fragment_instances: BTreeSet<UniqueId>,
    ) -> RuntimeFilterParticipantInstall {
        let channel_id = ChannelId::new(1);
        let witness_id = CoverageWitnessId::new(2);
        let is_live = matches!(activation, ConsumerActivation::NonBlockingLive { .. });
        let coverage = if is_live {
            Coverage::AllOf(vec![Coverage::Leaf(witness_id)])
        } else {
            Coverage::Leaf(witness_id)
        };
        let deployment = RuntimeFilterChannelDeployment::new(
            channel_id,
            RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: if is_live {
                    NullSemantics::NullSafeEqual
                } else {
                    NullSemantics::NeverMatches
                },
            },
            RuntimeFilterLifecycle::CompleteOnce,
            coverage.clone(),
            coverage,
            ReductionRequirement::SetUnion,
            if is_live {
                BTreeSet::from([
                    ContributionKind::FinalDomainShard,
                    ContributionKind::ProducerClosed,
                ])
            } else {
                BTreeSet::from([
                    ContributionKind::ValueDomainDelta,
                    ContributionKind::ProducerClosed,
                ])
            },
            if is_live {
                CompletionRequirement::FencedFinalDomain(CompletionFenceKind::CommittedDomainFrozen)
            } else {
                CompletionRequirement::ProducerClosed
            },
            RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 1024,
                max_artifact_bytes: 1024,
                deadline_ms: 100,
                max_retries: 1,
            },
            RuntimeFilterCoreBudget::new(8192),
            crate::runtime_filter::port::install::MaterializationPolicy::for_test(),
            BTreeMap::from([(
                BindingId::new(3),
                ProducerDeployment::new(witness_id, expected_fragment_instances),
            )]),
            BTreeMap::from([(
                BindingId::new(4),
                ConsumerDeployment::new(
                    activation,
                    capabilities,
                    BTreeSet::from([RouteEdgeId::new(5)]),
                    BTreeSet::from([uid(40)]),
                ),
            )]),
        );
        local_participant_install_for_test(RuntimeFilterInstallView::new(
            DeploymentEpoch::new(6),
            RuntimeFilterParticipantId::new(7),
            BTreeMap::from([(channel_id, deployment)]),
        ))
    }

    fn register(manager: &QueryContextManager, query_id: QueryId) {
        manager
            .get_or_register(
                query_id,
                false,
                Duration::from_secs(1),
                Duration::from_secs(5),
            )
            .expect("query context");
    }

    fn service(manager: &QueryContextManager, query_id: QueryId) -> Arc<RuntimeFilterService> {
        let guard = manager.inner.lock().expect("query manager");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .expect("query context")
            .runtime_filter_service()
    }

    struct LockProbeSink {
        manager: Weak<QueryContextManager>,
        terminal_probe: Mutex<Option<mpsc::SyncSender<bool>>>,
    }

    struct BlockingShutdownSink {
        entered: mpsc::SyncSender<()>,
        release: Mutex<mpsc::Receiver<()>>,
    }

    impl RuntimeFilterEventSink for BlockingShutdownSink {
        fn record(&self, event: RuntimeFilterEvent) {
            if !matches!(event, RuntimeFilterEvent::ChannelCancelled { .. }) {
                return;
            }
            self.entered.send(()).expect("shutdown entered");
            self.release
                .lock()
                .expect("shutdown release")
                .recv_timeout(Duration::from_secs(5))
                .expect("shutdown released");
        }
    }

    impl RuntimeFilterEventSink for LockProbeSink {
        fn record(&self, event: RuntimeFilterEvent) {
            if !matches!(event, RuntimeFilterEvent::ChannelCancelled { .. }) {
                return;
            }
            let lock_was_free = self
                .manager
                .upgrade()
                .is_some_and(|manager| manager.inner.try_lock().is_ok());
            if let Some(sender) = self.terminal_probe.lock().expect("probe sender").take() {
                let _ = sender.send(lock_was_free);
            }
        }
    }

    fn install_probed_service(
        manager: &Arc<QueryContextManager>,
        query_id: QueryId,
    ) -> (Arc<RuntimeFilterService>, mpsc::Receiver<bool>) {
        let (sender, receiver) = mpsc::sync_channel(1);
        let sink = Arc::new(LockProbeSink {
            manager: Arc::downgrade(manager),
            terminal_probe: Mutex::new(Some(sender)),
        });
        let service = {
            let mut guard = manager.inner.lock().expect("query manager");
            let context = guard.active.get_mut(&query_id).expect("active query");
            let service = Arc::new(RuntimeFilterService::new_for_query(
                uid(query_id.lo),
                sink,
                &context.mem_tracker,
            ));
            context.runtime_filter_service = service.clone();
            service
        };
        assert_eq!(
            service
                .install(participant_install())
                .expect("valid install"),
            InstallOutcome::Installed
        );
        (service, receiver)
    }

    fn assert_terminal_probe(receiver: mpsc::Receiver<bool>) {
        assert!(
            receiver
                .recv_timeout(Duration::from_secs(1))
                .expect("shutdown event"),
            "runtime filter shutdown must run after releasing the manager lock"
        );
    }

    #[test]
    fn runtime_filter_ingress_lookup_returns_active_service_without_mutation() {
        let manager = QueryContextManager::new_for_test();
        let query_id = query_id(9101);
        manager
            .ensure_context(
                query_id,
                false,
                Duration::from_secs(10),
                Duration::from_secs(10),
            )
            .expect("query context");
        let (before_service, before_fragments, before_deadlines) = {
            let guard = manager.inner.lock().expect("query manager");
            let context = guard.active.get(&query_id).expect("active query");
            (
                context.runtime_filter_service(),
                (context.num_fragments, context.num_active_fragments),
                (context.delivery_deadline, context.query_deadline),
            )
        };

        let found = manager
            .runtime_filter_service_for_ingress(query_id)
            .expect("active query service");

        assert!(Arc::ptr_eq(&before_service, &found));
        let guard = manager.inner.lock().expect("query manager");
        let context = guard.active.get(&query_id).expect("active query");
        assert_eq!(
            (context.num_fragments, context.num_active_fragments),
            before_fragments
        );
        assert_eq!(
            (context.delivery_deadline, context.query_deadline),
            before_deadlines
        );
        assert!(!guard.second_chance.contains_key(&query_id));
    }

    #[test]
    fn runtime_filter_ingress_lookup_returns_unexpired_second_chance_without_mutation() {
        let manager = QueryContextManager::new_for_test();
        let query_id = query_id(9102);
        register(&manager, query_id);
        manager.finish_fragment(query_id);
        let (before_service, before_fragments, before_deadlines) = {
            let guard = manager.inner.lock().expect("query manager");
            let context = guard
                .second_chance
                .get(&query_id)
                .expect("second-chance query");
            (
                context.runtime_filter_service(),
                (context.num_fragments, context.num_active_fragments),
                (context.delivery_deadline, context.query_deadline),
            )
        };

        let found = manager
            .runtime_filter_service_for_ingress(query_id)
            .expect("second-chance query service");

        assert!(Arc::ptr_eq(&before_service, &found));
        let guard = manager.inner.lock().expect("query manager");
        assert!(!guard.active.contains_key(&query_id));
        let context = guard
            .second_chance
            .get(&query_id)
            .expect("second-chance query");
        assert_eq!(
            (context.num_fragments, context.num_active_fragments),
            before_fragments
        );
        assert_eq!(
            (context.delivery_deadline, context.query_deadline),
            before_deadlines
        );
    }

    #[test]
    fn runtime_filter_ingress_lookup_rejects_expired_second_chance_without_mutation() {
        let manager = QueryContextManager::new_for_test();
        let query_id = query_id(9103);
        register(&manager, query_id);
        manager.finish_fragment(query_id);
        let (before_service, before_fragments, before_deadlines) = {
            let mut guard = manager.inner.lock().expect("query manager");
            let context = guard
                .second_chance
                .get_mut(&query_id)
                .expect("second-chance query");
            context.delivery_deadline = Instant::now() - Duration::from_millis(1);
            (
                context.runtime_filter_service(),
                (context.num_fragments, context.num_active_fragments),
                (context.delivery_deadline, context.query_deadline),
            )
        };

        assert!(
            manager
                .runtime_filter_service_for_ingress(query_id)
                .is_none()
        );

        let guard = manager.inner.lock().expect("query manager");
        assert!(!guard.active.contains_key(&query_id));
        let context = guard
            .second_chance
            .get(&query_id)
            .expect("second-chance query");
        assert!(Arc::ptr_eq(
            &before_service,
            &context.runtime_filter_service()
        ));
        assert_eq!(
            (context.num_fragments, context.num_active_fragments),
            before_fragments
        );
        assert_eq!(
            (context.delivery_deadline, context.query_deadline),
            before_deadlines
        );
    }

    #[test]
    fn runtime_filter_ingress_lookup_absent_query_does_not_mutate_manager() {
        let manager = QueryContextManager::new_for_test();
        let active_query_id = query_id(9104);
        let second_chance_query_id = query_id(9105);
        let absent_query_id = query_id(9106);
        let finst_id = uid(9107);
        register(&manager, active_query_id);
        register(&manager, second_chance_query_id);
        manager.finish_fragment(second_chance_query_id);
        manager.register_finst(finst_id, active_query_id);
        let before = {
            let guard = manager.inner.lock().expect("query manager");
            let active = guard.active.get(&active_query_id).expect("active query");
            let second_chance = guard
                .second_chance
                .get(&second_chance_query_id)
                .expect("second-chance query");
            (
                (
                    guard.active.len(),
                    guard.second_chance.len(),
                    guard.finst_to_query.len(),
                ),
                (
                    active.num_fragments,
                    active.num_active_fragments,
                    active.delivery_deadline,
                    active.query_deadline,
                ),
                (
                    second_chance.num_fragments,
                    second_chance.num_active_fragments,
                    second_chance.delivery_deadline,
                    second_chance.query_deadline,
                ),
            )
        };

        assert!(
            manager
                .runtime_filter_service_for_ingress(absent_query_id)
                .is_none()
        );

        let guard = manager.inner.lock().expect("query manager");
        let active = guard.active.get(&active_query_id).expect("active query");
        let second_chance = guard
            .second_chance
            .get(&second_chance_query_id)
            .expect("second-chance query");
        assert_eq!(
            (
                guard.active.len(),
                guard.second_chance.len(),
                guard.finst_to_query.len(),
            ),
            before.0
        );
        assert_eq!(
            (
                active.num_fragments,
                active.num_active_fragments,
                active.delivery_deadline,
                active.query_deadline,
            ),
            before.1
        );
        assert_eq!(
            (
                second_chance.num_fragments,
                second_chance.num_active_fragments,
                second_chance.delivery_deadline,
                second_chance.query_deadline,
            ),
            before.2
        );
        assert_eq!(
            guard
                .finst_to_query
                .get(&finst_id)
                .map(|execution| execution.query_id()),
            Some(active_query_id)
        );
    }

    #[test]
    fn runtime_filter_ingress_lookup_rejects_expired_claim_only_active_context() {
        let manager = QueryContextManager::new_for_test();
        let query_id = query_id(9108);
        manager
            .ensure_context(
                query_id,
                false,
                Duration::from_secs(10),
                Duration::from_secs(10),
            )
            .expect("claim-only query context");
        let (before_service, before_fragments, before_deadlines) = {
            let mut guard = manager.inner.lock().expect("query manager");
            let context = guard.active.get_mut(&query_id).expect("active query");
            context.query_deadline = Instant::now() - Duration::from_millis(1);
            (
                context.runtime_filter_service(),
                (context.num_fragments, context.num_active_fragments),
                (context.delivery_deadline, context.query_deadline),
            )
        };

        assert!(
            manager
                .runtime_filter_service_for_ingress(query_id)
                .is_none()
        );

        let guard = manager.inner.lock().expect("query manager");
        let context = guard.active.get(&query_id).expect("active query");
        assert!(Arc::ptr_eq(
            &before_service,
            &context.runtime_filter_service()
        ));
        assert_eq!(
            (context.num_fragments, context.num_active_fragments),
            before_fragments
        );
        assert_eq!(
            (context.delivery_deadline, context.query_deadline),
            before_deadlines
        );
    }

    #[test]
    fn runtime_filter_ingress_lookup_returns_active_fragment_past_query_deadline() {
        let manager = QueryContextManager::new_for_test();
        let query_id = query_id(9109);
        register(&manager, query_id);
        let (before_service, before_fragments, before_deadlines) = {
            let mut guard = manager.inner.lock().expect("query manager");
            let context = guard.active.get_mut(&query_id).expect("active query");
            context.query_deadline = Instant::now() - Duration::from_millis(1);
            (
                context.runtime_filter_service(),
                (context.num_fragments, context.num_active_fragments),
                (context.delivery_deadline, context.query_deadline),
            )
        };

        let found = manager
            .runtime_filter_service_for_ingress(query_id)
            .expect("active fragment service");

        assert!(Arc::ptr_eq(&before_service, &found));
        let guard = manager.inner.lock().expect("query manager");
        let context = guard.active.get(&query_id).expect("active query");
        assert_eq!(
            (context.num_fragments, context.num_active_fragments),
            before_fragments
        );
        assert_eq!(
            (context.delivery_deadline, context.query_deadline),
            before_deadlines
        );
    }

    #[test]
    fn runtime_filter_ingress_lookup_cleanup_wins_without_recreating_context() {
        let manager = QueryContextManager::new_for_test();
        let query_id = query_id(9110);
        manager
            .ensure_context(
                query_id,
                false,
                Duration::from_secs(10),
                Duration::from_secs(10),
            )
            .expect("claim-only query context");
        let barrier = Arc::new(Barrier::new(2));
        let mut guard = manager.inner.lock().expect("query manager");
        let context = guard.active.get_mut(&query_id).expect("active query");
        context.query_deadline = Instant::now() - Duration::from_millis(1);
        let before_service = context.runtime_filter_service();
        let before_fragments = (context.num_fragments, context.num_active_fragments);
        let before_deadlines = (context.delivery_deadline, context.query_deadline);
        let (lookup_tx, lookup_rx) = mpsc::sync_channel(1);
        let lookup = {
            let manager = Arc::clone(&manager);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                lookup_tx
                    .send(
                        manager
                            .runtime_filter_service_for_ingress(query_id)
                            .is_none(),
                    )
                    .expect("lookup result receiver");
            })
        };
        barrier.wait();
        let removed = guard.active.remove(&query_id).expect("expired query");
        drop(guard);

        removed.runtime_filter_service().shutdown();
        assert_eq!(
            (removed.num_fragments, removed.num_active_fragments),
            before_fragments
        );
        assert_eq!(
            (removed.delivery_deadline, removed.query_deadline),
            before_deadlines
        );
        drop(removed);
        manager.remove_runtime_filter_lifecycle_if_context_absent(query_id);

        assert!(
            lookup_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("lookup must complete")
        );
        lookup.join().expect("lookup thread");
        let guard = manager.inner.lock().expect("query manager");
        assert!(!guard.active.contains_key(&query_id));
        assert!(!guard.second_chance.contains_key(&query_id));
        assert_eq!(
            guard
                .finst_to_query
                .values()
                .filter(|execution| execution.query_id() == query_id)
                .count(),
            0
        );
        assert_eq!(Arc::strong_count(&before_service), 1);
    }

    #[test]
    fn query_context_constructs_exactly_one_runtime_filter_service() {
        let context = QueryContext::new(query_id(1), Duration::ZERO, Duration::ZERO);
        let first = context.runtime_filter_service();
        let second = context.runtime_filter_service();

        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn second_chance_round_trip_preserves_runtime_filter_service() {
        let manager = test_manager();
        let query_id = query_id(2);
        register(&manager, query_id);
        let before = service(&manager, query_id);

        manager.finish_fragment(query_id);
        manager
            .get_or_register(
                query_id,
                true,
                Duration::from_secs(1),
                Duration::from_secs(5),
            )
            .expect("second-chance query context");

        assert!(Arc::ptr_eq(&before, &service(&manager, query_id)));
        assert_eq!(
            before
                .install(participant_install())
                .expect("service remains open"),
            InstallOutcome::Installed
        );
    }

    #[test]
    fn empty_query_service_creates_no_channel_or_event() {
        let query_id = query_id(3);
        let query_key = QueryKey::from_hi_lo(query_id.hi, query_id.lo);
        let registry = RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);
        let context = QueryContext::new(query_id, Duration::ZERO, Duration::ZERO);
        let service = context.runtime_filter_service();
        assert_eq!(
            service
                .install(local_participant_install_for_test(
                    RuntimeFilterInstallView::new(
                        DeploymentEpoch::new(1),
                        RuntimeFilterParticipantId::new(1),
                        BTreeMap::new(),
                    ),
                ))
                .expect("empty view"),
            InstallOutcome::IgnoredEmpty
        );
        let snapshot = registry.snapshot(query_key).expect("query lifecycle entry");
        assert!(snapshot.filters.is_empty());
        assert!(snapshot.channel_events.is_empty());
        registry.remove_query(query_key);
    }

    #[test]
    fn cancel_query_cancels_service_after_releasing_manager_lock() {
        let manager = test_manager();
        let query_id = query_id(4);
        register(&manager, query_id);
        let (_service, receiver) = install_probed_service(&manager, query_id);

        manager.cancel_query(query_id, "cancelled".to_string());

        assert_terminal_probe(receiver);
    }

    #[test]
    fn abort_query_cancels_service_after_releasing_manager_lock() {
        let manager = test_manager();
        let query_id = query_id(5);
        register(&manager, query_id);
        let (_service, receiver) = install_probed_service(&manager, query_id);

        manager.abort_query(query_id);

        assert_terminal_probe(receiver);
    }

    #[test]
    fn dead_finish_shuts_down_and_drops_context_after_releasing_manager_lock() {
        let manager = test_manager();
        let query_id = query_id(6);
        register(&manager, query_id);
        let (_service, receiver) = install_probed_service(&manager, query_id);
        manager
            .inner
            .lock()
            .expect("query manager")
            .active
            .get_mut(&query_id)
            .expect("active query")
            .total_fragments = Some(1);

        manager.finish_fragment(query_id);

        assert_terminal_probe(receiver);
        assert!(
            !manager
                .inner
                .lock()
                .expect("query manager")
                .active
                .contains_key(&query_id)
        );
    }

    #[test]
    fn clean_expired_shuts_down_second_chance_context_after_releasing_manager_lock() {
        let manager = test_manager();
        let query_id = query_id(7);
        register(&manager, query_id);
        let (_service, receiver) = install_probed_service(&manager, query_id);
        {
            let mut guard = manager.inner.lock().expect("query manager");
            let mut context = guard.active.remove(&query_id).expect("active query");
            context.num_active_fragments = 0;
            context.delivery_deadline = Instant::now() - Duration::from_millis(1);
            guard.second_chance.insert(query_id, context);
        }

        manager.clean_expired();

        assert_terminal_probe(receiver);
        assert!(
            !manager
                .inner
                .lock()
                .expect("query manager")
                .second_chance
                .contains_key(&query_id)
        );
    }

    #[test]
    fn clean_expired_shuts_down_expired_claim_only_active_context_after_releasing_manager_lock() {
        let manager = test_manager();
        let query_id = query_id(72);
        let query_key = QueryKey::from_hi_lo(query_id.hi, query_id.lo);
        let registry = RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);
        manager
            .ensure_native_context(
                query_id,
                false,
                Duration::from_secs(1),
                Duration::from_secs(5),
            )
            .expect("runtime-filter RPC claim-only context");
        let (service, receiver) = install_probed_service(&manager, query_id);
        manager
            .inner
            .lock()
            .expect("query manager")
            .active
            .get_mut(&query_id)
            .expect("claim-only active query")
            .query_deadline = Instant::now() - Duration::from_millis(1);

        manager.clean_expired();

        assert_terminal_probe(receiver);
        assert!(
            !manager
                .inner
                .lock()
                .expect("query manager")
                .active
                .contains_key(&query_id)
        );
        assert!(registry.snapshot(query_key).is_none());
        let error = service
            .install(participant_install())
            .expect_err("expired claim-only service must remain closed");
        assert_eq!(error.kind(), InstallContractErrorKind::ServiceClosed);
    }

    #[test]
    fn clean_expired_preserves_unexpired_claim_only_active_context() {
        let manager = test_manager();
        let query_id = query_id(73);
        manager
            .ensure_native_context(
                query_id,
                false,
                Duration::from_secs(1),
                Duration::from_secs(5),
            )
            .expect("runtime-filter RPC claim-only context");
        let before = service(&manager, query_id);

        manager.clean_expired();

        let guard = manager.inner.lock().expect("query manager");
        let context = guard.active.get(&query_id).expect("claim remains active");
        assert_eq!(context.num_active_fragments, 0);
        assert!(Arc::ptr_eq(&before, &context.runtime_filter_service()));
    }

    #[test]
    fn clean_expired_preserves_active_fragment_past_query_deadline() {
        let manager = test_manager();
        let query_id = query_id(74);
        manager
            .get_or_register_native(
                query_id,
                false,
                Duration::from_secs(1),
                Duration::from_secs(5),
            )
            .expect("active Native fragment");
        manager
            .inner
            .lock()
            .expect("query manager")
            .active
            .get_mut(&query_id)
            .expect("active query")
            .query_deadline = Instant::now() - Duration::from_millis(1);

        manager.clean_expired();

        let guard = manager.inner.lock().expect("query manager");
        let context = guard
            .active
            .get(&query_id)
            .expect("active fragment retained");
        assert_eq!(context.num_active_fragments, 1);
    }

    #[test]
    fn concurrent_native_fragment_recreates_context_while_expired_claim_shuts_down() {
        let manager = test_manager();
        let query_id = query_id(75);
        let query_key = QueryKey::from_hi_lo(query_id.hi, query_id.lo);
        let registry = RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);
        manager
            .ensure_native_context(
                query_id,
                false,
                Duration::from_secs(1),
                Duration::from_secs(5),
            )
            .expect("claim-only Native context");

        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let sink = Arc::new(BlockingShutdownSink {
            entered: entered_tx,
            release: Mutex::new(release_rx),
        });
        let old_service = {
            let mut guard = manager.inner.lock().expect("query manager");
            let context = guard.active.get_mut(&query_id).expect("claim-only query");
            context.query_deadline = Instant::now() - Duration::from_millis(1);
            let service = Arc::new(RuntimeFilterService::new_for_query(
                uid(query_id.lo),
                sink,
                &context.mem_tracker,
            ));
            context.runtime_filter_service = Arc::clone(&service);
            service
        };
        assert_eq!(
            old_service
                .install(participant_install())
                .expect("valid install"),
            InstallOutcome::Installed
        );

        let cleaner = {
            let manager = Arc::clone(&manager);
            std::thread::spawn(move || manager.clean_expired())
        };
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("expired claim shutdown must start");

        manager
            .get_or_register_native(
                query_id,
                false,
                Duration::from_secs(1),
                Duration::from_secs(5),
            )
            .expect("legal concurrent Native fragment");
        release_tx.send(()).expect("release old shutdown");
        cleaner.join().expect("cleaner");

        let guard = manager.inner.lock().expect("query manager");
        let context = guard.active.get(&query_id).expect("replacement context");
        assert_eq!(context.num_active_fragments, 1);
        assert!(registry.snapshot(query_key).is_some());
        registry.remove_query(query_key);
    }

    #[test]
    fn clean_expired_preserves_lifecycle_recreated_while_old_context_shuts_down() {
        let manager = test_manager();
        let query_id = query_id(71);
        let query_key = QueryKey::from_hi_lo(query_id.hi, query_id.lo);
        let registry = RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);
        register(&manager, query_id);

        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let sink = Arc::new(BlockingShutdownSink {
            entered: entered_tx,
            release: Mutex::new(release_rx),
        });
        {
            let mut guard = manager.inner.lock().expect("query manager");
            let context = guard.active.get_mut(&query_id).expect("active query");
            let old_service = Arc::new(RuntimeFilterService::new_for_query(
                uid(query_id.lo),
                sink,
                &context.mem_tracker,
            ));
            context.runtime_filter_service = old_service.clone();
            assert_eq!(
                old_service
                    .install(participant_install())
                    .expect("valid install"),
                InstallOutcome::Installed
            );

            let mut context = guard.active.remove(&query_id).expect("active query");
            context.num_active_fragments = 0;
            context.delivery_deadline = Instant::now() - Duration::from_millis(1);
            guard.second_chance.insert(query_id, context);
        }

        let cleaner = {
            let manager = Arc::clone(&manager);
            std::thread::spawn(move || manager.clean_expired())
        };
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("old shutdown must block in the sink");

        manager
            .ensure_context(
                query_id,
                false,
                Duration::from_secs(1),
                Duration::from_secs(5),
            )
            .expect("replacement query context");
        release_tx.send(()).expect("release old shutdown");
        cleaner.join().expect("cleaner");

        assert!(
            registry.snapshot(query_key).is_some(),
            "old cleanup must not remove the replacement context lifecycle"
        );
        registry.remove_query(query_key);
    }

    #[test]
    fn late_service_handle_cannot_recreate_deployment_after_shutdown() {
        let manager = test_manager();
        let query_id = query_id(8);
        register(&manager, query_id);
        let (service, receiver) = install_probed_service(&manager, query_id);
        manager.cancel_query(query_id, "cancelled".to_string());
        assert_terminal_probe(receiver);

        let error = service
            .install(participant_install())
            .expect_err("closed service");
        assert_eq!(error.kind(), InstallContractErrorKind::ServiceClosed);
    }

    #[test]
    fn query_context_drop_shuts_down_service_retained_by_external_handle() {
        let context = QueryContext::new(query_id(9), Duration::ZERO, Duration::ZERO);
        let service = context.runtime_filter_service();
        assert_eq!(
            service
                .install(participant_install())
                .expect("valid install"),
            InstallOutcome::Installed
        );

        drop(context);

        let error = service
            .install(participant_install())
            .expect_err("context drop must close retained service");
        assert_eq!(error.kind(), InstallContractErrorKind::ServiceClosed);
    }
}

#[cfg(test)]
mod tests {
    pub(super) mod runtime_filter_tick {
        use std::collections::VecDeque;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use std::sync::{Arc, Mutex, Weak, mpsc};
        use std::time::{Duration, Instant};

        use super::super::{QueryContextManager, QueryId};
        use crate::common::types::UniqueId;
        use crate::runtime::endpoint::RuntimeEndpoint;
        use crate::runtime_filter::codec::artifact::EncodedArtifactFrame;
        use crate::runtime_filter::model::contract::{BindingId, ChannelId};
        use crate::runtime_filter::port::events::{
            RuntimeFilterEvent, RuntimeFilterEventIdentity, RuntimeFilterEventSink,
            TransportRouteEventIdentity,
        };
        use crate::runtime_filter::port::identity::{
            DeploymentEpoch, ProducerSequence, RouteEdgeId, RuntimeFilterParticipantId,
        };
        use crate::runtime_filter::port::routing::{
            RuntimeFilterRemoteRoute, RuntimeFilterRouteRole,
        };
        use crate::runtime_filter::port::support::{
            MemoryAccountError, RuntimeFilterClock, RuntimeFilterMemoryAccount,
        };
        use crate::runtime_filter::port::transport::{
            DeliveryRouteIdentity, RuntimeFilterAcceptStatus, RuntimeFilterRouteIdentity,
            RuntimeFilterTransportEnvelope,
        };
        use crate::runtime_filter::router::remote::{
            RuntimeFilterEnvelopeSink, SinkCompletion, SinkSubmitOutcome,
        };
        use crate::runtime_filter::service::{ReliableTransportPolicy, RuntimeFilterService};

        const WAIT: Duration = Duration::from_secs(1);
        const RETRY: Duration = Duration::from_millis(10);
        const DEADLINE: Duration = Duration::from_millis(20);

        struct ManualClock(Mutex<Instant>);

        impl RuntimeFilterClock for ManualClock {
            fn now(&self) -> Instant {
                *self.0.lock().expect("manual clock")
            }
        }

        struct NoopEvents;

        impl RuntimeFilterEventSink for NoopEvents {
            fn record(&self, _event: RuntimeFilterEvent) {}
        }

        struct NoopMemory;

        impl RuntimeFilterMemoryAccount for NoopMemory {
            fn try_consume(&self, _bytes: usize) -> Result<(), MemoryAccountError> {
                Ok(())
            }

            fn release(&self, _bytes: usize) {}
        }

        struct TickSink {
            attempts: AtomicUsize,
            recorded: AtomicUsize,
            shutdown: AtomicBool,
            completions: Mutex<VecDeque<SinkCompletion>>,
            lock_probe: Mutex<Option<(Weak<QueryContextManager>, QueryId, mpsc::SyncSender<bool>)>>,
            retry_gate: Mutex<Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>>,
            shutdown_gate: Mutex<Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>>,
        }

        impl TickSink {
            fn plain() -> Arc<Self> {
                Arc::new(Self {
                    attempts: AtomicUsize::new(0),
                    recorded: AtomicUsize::new(0),
                    shutdown: AtomicBool::new(false),
                    completions: Mutex::new(VecDeque::new()),
                    lock_probe: Mutex::new(None),
                    retry_gate: Mutex::new(None),
                    shutdown_gate: Mutex::new(None),
                })
            }

            fn with_lock_probe(
                manager: &Arc<QueryContextManager>,
                query_id: QueryId,
            ) -> (Arc<Self>, mpsc::Receiver<bool>) {
                let sink = Self::plain();
                let (probe_tx, probe_rx) = mpsc::sync_channel(1);
                *sink.lock_probe.lock().expect("lock probe") =
                    Some((Arc::downgrade(manager), query_id, probe_tx));
                (sink, probe_rx)
            }

            fn with_blocked_retry() -> (Arc<Self>, mpsc::Receiver<()>, mpsc::SyncSender<()>) {
                let sink = Self::plain();
                let (entered_tx, entered_rx) = mpsc::sync_channel(1);
                let (release_tx, release_rx) = mpsc::sync_channel(1);
                *sink.retry_gate.lock().expect("retry gate") = Some((entered_tx, release_rx));
                (sink, entered_rx, release_tx)
            }

            fn with_blocked_shutdown() -> (Arc<Self>, mpsc::Receiver<()>, mpsc::SyncSender<()>) {
                let sink = Self::plain();
                let (entered_tx, entered_rx) = mpsc::sync_channel(1);
                let (release_tx, release_rx) = mpsc::sync_channel(1);
                *sink.shutdown_gate.lock().expect("shutdown gate") = Some((entered_tx, release_rx));
                (sink, entered_rx, release_tx)
            }

            fn send_count(&self) -> usize {
                self.recorded.load(Ordering::Acquire)
            }

            fn push_completion(&self, completion: SinkCompletion) {
                self.completions
                    .lock()
                    .expect("completion queue")
                    .push_back(completion);
            }
        }

        impl RuntimeFilterEnvelopeSink for TickSink {
            fn try_send(
                &self,
                _route: RuntimeFilterRemoteRoute,
                _envelope: RuntimeFilterTransportEnvelope,
            ) -> SinkSubmitOutcome {
                let attempt = self.attempts.fetch_add(1, Ordering::AcqRel) + 1;
                if attempt == 2
                    && let Some((entered, release)) =
                        self.retry_gate.lock().expect("retry gate").take()
                {
                    entered.send(()).expect("retry entered");
                    release.recv_timeout(WAIT).expect("retry released");
                }
                self.recorded.fetch_add(1, Ordering::AcqRel);
                SinkSubmitOutcome::Submitted
            }

            fn try_recv_completion(&self) -> Option<SinkCompletion> {
                if let Some((manager, query_id, sender)) =
                    self.lock_probe.lock().expect("lock probe").take()
                {
                    let lock_was_free = manager.upgrade().is_some_and(|manager| {
                        let lock_was_free = manager.inner.try_lock().is_ok();
                        lock_was_free
                            && manager
                                .runtime_filter_service_for_ingress(query_id)
                                .is_some()
                    });
                    sender.send(lock_was_free).expect("lock probe result");
                }
                self.completions
                    .lock()
                    .expect("completion queue")
                    .pop_front()
            }

            fn shutdown(&self) {
                if let Some((entered, release)) =
                    self.shutdown_gate.lock().expect("shutdown gate").take()
                {
                    entered.send(()).expect("shutdown entered");
                    release.recv_timeout(WAIT).expect("shutdown released");
                }
                self.shutdown.store(true, Ordering::Release);
            }
        }

        fn query_id(lo: i64) -> QueryId {
            QueryId { hi: 97, lo }
        }

        fn route() -> RuntimeFilterRemoteRoute {
            RuntimeFilterRemoteRoute::new(
                RouteEdgeId::new(11),
                RuntimeFilterParticipantId::new(12),
                RuntimeEndpoint::new("127.0.0.1", 19060).expect("endpoint"),
                RuntimeFilterRouteRole::Consumer(BindingId::new(13)),
            )
            .expect("remote route")
        }

        fn identity() -> TransportRouteEventIdentity {
            TransportRouteEventIdentity::new(
                RuntimeFilterEventIdentity::new(
                    UniqueId { hi: 97, lo: 1 },
                    RuntimeFilterParticipantId::new(14),
                    ChannelId::new(15),
                    DeploymentEpoch::new(16),
                ),
                RouteEdgeId::new(11),
            )
        }

        fn install_service(
            manager: &Arc<QueryContextManager>,
            query_id: QueryId,
            started: Instant,
            sink: Arc<TickSink>,
        ) -> Arc<RuntimeFilterService> {
            manager
                .ensure_native_context(
                    query_id,
                    false,
                    Duration::from_secs(30),
                    Duration::from_secs(30),
                )
                .expect("native context");
            let service = Arc::new(RuntimeFilterService::new_for_lifecycle_test(
                UniqueId {
                    hi: query_id.hi,
                    lo: query_id.lo,
                },
                Arc::new(ManualClock(Mutex::new(started))),
                Arc::new(NoopEvents),
                Arc::new(NoopMemory),
            ));
            service.set_remote_sink_for_test(sink);
            service
                .configure_transport(ReliableTransportPolicy::new(RETRY, 2, DEADLINE, 8, 4096))
                .expect("transport policy");
            let mut guard = manager.inner.lock().expect("query manager");
            guard
                .active
                .get_mut(&query_id)
                .expect("active context")
                .runtime_filter_service = service.clone();
            service
        }

        fn seed_pending(service: &RuntimeFilterService) {
            service.seed_remote_transport_for_test(
                &route(),
                Arc::new(EncodedArtifactFrame::from_parts_for_test(
                    [9; 32],
                    vec![1, 2, 3],
                )),
                identity(),
            );
            assert_eq!(service.transport_pending_len_for_test(), 1);
        }

        fn set_manager_now(manager: &QueryContextManager, now: Instant) {
            manager
                .inner
                .lock()
                .expect("query manager")
                .runtime_filter_terminal_now = Some(now);
        }

        fn run_cleanup_joined(manager: Arc<QueryContextManager>) {
            let (done_tx, done_rx) = mpsc::sync_channel(1);
            let cleanup = std::thread::spawn(move || {
                manager.clean_expired_for_test();
                done_tx.send(()).expect("cleanup complete");
            });
            done_rx.recv_timeout(WAIT).expect("cleanup joined in time");
            cleanup.join().expect("cleanup thread");
        }

        #[test]
        fn manager_tick_drives_retry_and_deadline_without_manual_test_hook() {
            let manager = QueryContextManager::new_for_test();
            let query_id = query_id(1);
            let started = Instant::now();
            set_manager_now(&manager, started);
            let sink = TickSink::plain();
            let service = install_service(&manager, query_id, started, sink.clone());
            seed_pending(&service);
            assert_eq!(sink.send_count(), 1);

            set_manager_now(&manager, started + RETRY);
            run_cleanup_joined(manager.clone());
            assert_eq!(sink.send_count(), 2, "manager tick must drive one retry");
            assert_eq!(service.transport_pending_len_for_test(), 1);

            set_manager_now(&manager, started + DEADLINE);
            run_cleanup_joined(manager);
            assert_eq!(service.transport_pending_len_for_test(), 0);
            assert_eq!(sink.send_count(), 2, "deadline must not send again");
        }

        #[test]
        fn manager_never_ticks_service_while_holding_inner_lock() {
            let manager = QueryContextManager::new_for_test();
            let query_id = query_id(2);
            let started = Instant::now();
            set_manager_now(&manager, started);
            let (sink, probe_rx) = TickSink::with_lock_probe(&manager, query_id);
            let _service = install_service(&manager, query_id, started, sink);

            run_cleanup_joined(manager);
            assert!(
                probe_rx.recv_timeout(WAIT).expect("tick lock probe"),
                "tick callback must be able to reenter the manager"
            );
        }

        #[test]
        fn cleanup_race_cannot_resurrect_or_send_after_abort() {
            let manager = QueryContextManager::new_for_test();
            let query_id = query_id(3);
            let epoch = DeploymentEpoch::new(16);
            let started = Instant::now();
            set_manager_now(&manager, started + RETRY);
            let (sink, retry_entered_rx, release_retry_tx) = TickSink::with_blocked_retry();
            let service = install_service(&manager, query_id, started, sink.clone());
            seed_pending(&service);

            let (cleanup_done_tx, cleanup_done_rx) = mpsc::sync_channel(1);
            let cleanup_manager = manager.clone();
            let cleanup = std::thread::spawn(move || {
                cleanup_manager.clean_expired_for_test();
                cleanup_done_tx.send(()).expect("cleanup complete");
            });
            retry_entered_rx
                .recv_timeout(WAIT)
                .expect("cleanup tick entered retry send");

            let (abort_done_tx, abort_done_rx) = mpsc::sync_channel(1);
            let abort_manager = manager.clone();
            let abort_sink = sink.clone();
            let abort = std::thread::spawn(move || {
                let result = abort_manager.abort_runtime_filter_deployment(query_id, epoch);
                abort_done_tx
                    .send((result, abort_sink.send_count()))
                    .expect("abort complete");
            });
            assert!(
                abort_done_rx
                    .recv_timeout(Duration::from_millis(100))
                    .is_err(),
                "abort must wait for the admitted retry submission"
            );
            release_retry_tx.send(()).expect("release retry");
            cleanup_done_rx
                .recv_timeout(WAIT)
                .expect("cleanup joined in time");
            let (abort_result, sends_at_abort) = abort_done_rx
                .recv_timeout(WAIT)
                .expect("abort joined in time");
            cleanup.join().expect("cleanup thread");
            abort.join().expect("abort thread");
            abort_result.expect("abort succeeds");

            assert_eq!(sink.send_count(), sends_at_abort, "no post-abort send");
            assert!(
                manager
                    .runtime_filter_service_for_ingress(query_id)
                    .is_none()
            );
            sink.push_completion(SinkCompletion::Ack(
                RuntimeFilterRouteIdentity::delivery(
                    DeliveryRouteIdentity::try_new(RouteEdgeId::new(11), ProducerSequence::new(1))
                        .expect("delivery identity"),
                ),
                RuntimeFilterAcceptStatus::Accepted,
            ));
            service.tick(started + DEADLINE + RETRY);
            assert_eq!(sink.send_count(), sends_at_abort, "late tick is terminal");
            assert_eq!(service.transport_pending_len_for_test(), 0);
            assert!(
                manager
                    .runtime_filter_service_for_ingress(query_id)
                    .is_none()
            );
        }

        #[test]
        fn manager_shutdown_drains_live_sink() {
            let manager = QueryContextManager::new_for_test();
            let query_id = query_id(4);
            let started = Instant::now();
            set_manager_now(&manager, started);
            let sink = TickSink::plain();
            let service = install_service(&manager, query_id, started, sink.clone());
            seed_pending(&service);
            {
                let mut guard = manager.inner.lock().expect("query manager");
                guard
                    .active
                    .get_mut(&query_id)
                    .expect("active context")
                    .query_deadline = Instant::now() - Duration::from_millis(1);
            }

            run_cleanup_joined(manager.clone());

            assert!(sink.shutdown.load(Ordering::Acquire));
            assert_eq!(service.transport_pending_len_for_test(), 0);
            assert!(
                manager
                    .runtime_filter_service_for_ingress(query_id)
                    .is_none()
            );
        }

        #[test]
        fn duplicate_shutdown_waits_for_the_first_teardown_to_close() {
            let manager = QueryContextManager::new_for_test();
            let query_id = query_id(5);
            let started = Instant::now();
            let (sink, shutdown_entered_rx, release_shutdown_tx) =
                TickSink::with_blocked_shutdown();
            let service = install_service(&manager, query_id, started, sink.clone());
            seed_pending(&service);

            let (first_done_tx, first_done_rx) = mpsc::sync_channel(1);
            let first_service = service.clone();
            let first = std::thread::spawn(move || {
                first_service.shutdown();
                first_done_tx.send(()).expect("first shutdown complete");
            });
            shutdown_entered_rx
                .recv_timeout(WAIT)
                .expect("first shutdown entered sink teardown");

            let (second_done_tx, second_done_rx) = mpsc::sync_channel(1);
            let second_service = service.clone();
            let second = std::thread::spawn(move || {
                second_service.cancel();
                second_done_tx.send(()).expect("second shutdown complete");
            });
            let second_returned_early = second_done_rx
                .recv_timeout(Duration::from_millis(100))
                .is_ok();

            release_shutdown_tx
                .send(())
                .expect("release first shutdown");
            first_done_rx
                .recv_timeout(WAIT)
                .expect("first shutdown joined in time");
            if !second_returned_early {
                second_done_rx
                    .recv_timeout(WAIT)
                    .expect("second shutdown joined in time");
            }
            first.join().expect("first shutdown thread");
            second.join().expect("second shutdown thread");

            assert!(
                !second_returned_early,
                "a duplicate shutdown must wait until the first teardown is closed"
            );
            assert!(sink.shutdown.load(Ordering::Acquire));
            assert_eq!(service.transport_pending_len_for_test(), 0);
        }
    }

    pub(super) mod runtime_filter_deployment {
        use std::sync::{Arc, Mutex, mpsc};
        use std::time::{Duration, Instant};

        use super::super::{
            LookupFetcherLifecycle, QueryContextManager, QueryExecutionKey,
            RuntimeFilterDeploymentAbortOutcome, RuntimeFilterDeploymentClaim,
            RuntimeFilterDeploymentClaimState, RuntimeFilterDeploymentCompletion,
            RuntimeFilterDeploymentInstallErrorKind, RuntimeFilterDeploymentInstallOutcome,
        };
        use crate::common::types::UniqueId;
        use crate::protocol::native::RuntimeFilterQueryLifecycleOptions;
        use crate::runtime_filter::port::identity::DeploymentEpoch;
        use crate::runtime_filter::port::install::RuntimeFilterParticipantInstall;

        use super::super::runtime_filter_service_lifecycle_tests::{participant_install, query_id};

        fn lifecycle() -> RuntimeFilterQueryLifecycleOptions {
            RuntimeFilterQueryLifecycleOptions {
                delivery_expire: Duration::from_secs(11),
                query_expire: Duration::from_secs(29),
                transport_retry_interval: Duration::from_millis(200),
                transport_max_attempts: 3,
                transport_deadline: Duration::from_secs(5),
                transport_max_pending_entries: 128,
                transport_max_pending_bytes: 1024 * 1024,
            }
        }

        #[test]
        fn native_execution_getter_returns_the_query_owned_installed_service() {
            let manager = QueryContextManager::new_for_test();
            let query = query_id(9_101);
            let finst = UniqueId { hi: 70, lo: 30 };
            manager
                .ensure_native_context(
                    query,
                    false,
                    Duration::from_secs(11),
                    Duration::from_secs(29),
                )
                .expect("native query context");
            let query_owned = {
                let guard = manager.inner.lock().expect("query manager");
                guard.active[&query].runtime_filter_service()
            };

            manager
                .install_runtime_filter_deployment(query, lifecycle(), participant_install())
                .expect("installed deployment");
            let native = manager
                .runtime_filter_context_for_native_execution(query, finst)
                .expect("strict native execution context");

            assert!(Arc::ptr_eq(native.service(), &query_owned));
            assert_eq!(
                native.query_id(),
                UniqueId {
                    hi: query.hi,
                    lo: query.lo
                }
            );
            assert_eq!(native.epoch(), DeploymentEpoch::new(6));
            assert_eq!(native.fragment_instance_id(), finst);
        }

        #[test]
        fn native_execution_getter_rejects_missing_install_cancelled_and_wrong_query() {
            let manager = QueryContextManager::new_for_test();
            let query = query_id(9_102);
            let finst = UniqueId { hi: 70, lo: 30 };
            manager
                .ensure_native_context(
                    query,
                    false,
                    Duration::from_secs(11),
                    Duration::from_secs(29),
                )
                .expect("native query context");

            let missing = manager
                .runtime_filter_context_for_native_execution(query, finst)
                .expect_err("deployment must already be installed");
            assert!(missing.contains("Installed"), "{missing}");

            let wrong = manager
                .runtime_filter_context_for_native_execution(query_id(9_999), finst)
                .expect_err("wrong query cannot borrow another query's service");
            assert!(wrong.contains("not found"), "{wrong}");

            manager.cancel_query(query, "cancelled for test".to_string());
            let cancelled = manager
                .runtime_filter_context_for_native_execution(query, finst)
                .expect_err("cancelled query cannot start native execution");
            assert!(cancelled.contains("cancelled"), "{cancelled}");
        }

        #[test]
        fn native_execution_getter_rejects_installing_deployment() {
            let manager = QueryContextManager::new_for_test();
            let query = query_id(9_103);
            let lifecycle = lifecycle();
            manager
                .ensure_native_context(
                    query,
                    false,
                    lifecycle.delivery_expire,
                    lifecycle.query_expire,
                )
                .expect("native query context");
            {
                let mut guard = manager.inner.lock().expect("query manager");
                let context = guard.active.get_mut(&query).expect("active query");
                context.runtime_filter_deployment = Some(RuntimeFilterDeploymentClaim {
                    state: RuntimeFilterDeploymentClaimState::Installing,
                    lifecycle,
                    install: participant_install(),
                    completion: Arc::new(RuntimeFilterDeploymentCompletion::new()),
                });
            }

            let error = manager
                .runtime_filter_context_for_native_execution(query, UniqueId { hi: 70, lo: 30 })
                .expect_err("installing deployment is not executable");
            assert!(error.contains("Installing"), "{error}");
        }

        fn with_epoch(
            install: RuntimeFilterParticipantInstall,
            epoch: u64,
        ) -> RuntimeFilterParticipantInstall {
            let (core, routing) = install.into_parts();
            let core = crate::runtime_filter::port::install::RuntimeFilterInstallView::new(
                DeploymentEpoch::new(epoch),
                core.local_participant_id(),
                core.channels().clone(),
            );
            let routing = crate::runtime_filter::port::routing::RuntimeFilterRoutingShard::new(
                DeploymentEpoch::new(epoch),
                routing.local_participant_id(),
                routing.channels().clone(),
            )
            .expect("valid routing shard with replacement epoch");
            RuntimeFilterParticipantInstall::new(core, routing)
        }

        fn snapshot(
            manager: &QueryContextManager,
            query_id: super::super::QueryId,
        ) -> Option<(usize, usize, Duration, Duration)> {
            let guard = manager.inner.lock().expect("query manager");
            guard
                .active
                .get(&query_id)
                .or_else(|| guard.second_chance.get(&query_id))
                .map(|context| {
                    (
                        context.num_fragments,
                        context.num_active_fragments,
                        context.delivery_expire,
                        context.query_expire,
                    )
                })
        }

        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        enum ContextLocation {
            Active,
            SecondChance,
        }

        fn location_snapshot(
            manager: &QueryContextManager,
            query_id: super::super::QueryId,
        ) -> Option<(
            ContextLocation,
            usize,
            usize,
            std::time::Instant,
            std::time::Instant,
        )> {
            let guard = manager.inner.lock().expect("query manager");
            guard
                .active
                .get(&query_id)
                .map(|context| {
                    (
                        ContextLocation::Active,
                        context.num_fragments,
                        context.num_active_fragments,
                        context.delivery_deadline,
                        context.query_deadline,
                    )
                })
                .or_else(|| {
                    guard.second_chance.get(&query_id).map(|context| {
                        (
                            ContextLocation::SecondChance,
                            context.num_fragments,
                            context.num_active_fragments,
                            context.delivery_deadline,
                            context.query_deadline,
                        )
                    })
                })
        }

        fn deployment_context_snapshot(
            manager: &QueryContextManager,
            query_id: super::super::QueryId,
        ) -> Option<(
            ContextLocation,
            usize,
            usize,
            std::time::Instant,
            std::time::Instant,
            Option<RuntimeFilterDeploymentClaimState>,
            usize,
        )> {
            let guard = manager.inner.lock().expect("query manager");
            let (location, context) = guard
                .active
                .get(&query_id)
                .map(|context| (ContextLocation::Active, context))
                .or_else(|| {
                    guard
                        .second_chance
                        .get(&query_id)
                        .map(|context| (ContextLocation::SecondChance, context))
                })?;
            Some((
                location,
                context.num_fragments,
                context.num_active_fragments,
                context.delivery_deadline,
                context.query_deadline,
                context
                    .runtime_filter_deployment
                    .as_ref()
                    .map(|claim| claim.state),
                Arc::as_ptr(&context.runtime_filter_service) as usize,
            ))
        }

        fn create_empty_context(
            manager: &QueryContextManager,
            query: super::super::QueryId,
            location: ContextLocation,
        ) {
            manager
                .ensure_native_context(
                    query,
                    false,
                    lifecycle().delivery_expire,
                    lifecycle().query_expire,
                )
                .expect("empty native context");
            if location == ContextLocation::SecondChance {
                let mut guard = manager.inner.lock().expect("query manager");
                let context = guard.active.remove(&query).expect("active empty context");
                guard.second_chance.insert(query, context);
            }
        }

        #[test]
        fn install_precreates_context_without_fragment_count() {
            let manager = QueryContextManager::new_for_test();
            let query = query_id(9201);
            assert_eq!(snapshot(&manager, query), None);

            let outcome = manager
                .install_runtime_filter_deployment(query, lifecycle(), participant_install())
                .expect("first install");

            assert_eq!(outcome, RuntimeFilterDeploymentInstallOutcome::Applied);
            assert_eq!(
                snapshot(&manager, query),
                Some((0, 0, lifecycle().delivery_expire, lifecycle().query_expire))
            );
        }

        #[test]
        fn identical_install_retry_is_idempotent() {
            let manager = QueryContextManager::new_for_test();
            let query = query_id(9202);
            let install = participant_install();
            assert_eq!(snapshot(&manager, query), None);
            assert_eq!(
                manager
                    .install_runtime_filter_deployment(query, lifecycle(), install.clone())
                    .expect("first install"),
                RuntimeFilterDeploymentInstallOutcome::Applied
            );
            let before = snapshot(&manager, query);
            assert_eq!(
                before,
                Some((0, 0, lifecycle().delivery_expire, lifecycle().query_expire))
            );

            assert_eq!(
                manager
                    .install_runtime_filter_deployment(query, lifecycle(), install)
                    .expect("identical retry"),
                RuntimeFilterDeploymentInstallOutcome::Idempotent
            );
            assert_eq!(snapshot(&manager, query), before);
        }

        #[test]
        fn conflicting_or_stale_install_is_rejected() {
            let manager = QueryContextManager::new_for_test();
            let query = query_id(9203);
            let install = participant_install();
            assert_eq!(snapshot(&manager, query), None);
            manager
                .install_runtime_filter_deployment(query, lifecycle(), install.clone())
                .expect("first install");
            let before = snapshot(&manager, query);
            assert_eq!(
                before,
                Some((0, 0, lifecycle().delivery_expire, lifecycle().query_expire))
            );

            let stale = manager
                .install_runtime_filter_deployment(
                    query,
                    lifecycle(),
                    with_epoch(install.clone(), 5),
                )
                .expect_err("older epoch");
            assert_eq!(
                stale.kind(),
                RuntimeFilterDeploymentInstallErrorKind::StaleEpoch
            );
            assert_eq!(snapshot(&manager, query), before);

            let mut changed_lifecycle = lifecycle();
            changed_lifecycle.query_expire += Duration::from_secs(1);
            let conflict = manager
                .install_runtime_filter_deployment(query, changed_lifecycle, install)
                .expect_err("same epoch lifecycle conflict");
            assert_eq!(
                conflict.kind(),
                RuntimeFilterDeploymentInstallErrorKind::ConflictingDeployment
            );
            assert_eq!(snapshot(&manager, query), before);
        }

        #[test]
        fn abort_is_idempotent_and_leaves_tombstone() {
            let manager = QueryContextManager::new_for_test();
            let query = query_id(9204);
            let install = participant_install();
            assert_eq!(snapshot(&manager, query), None);
            manager
                .install_runtime_filter_deployment(query, lifecycle(), install.clone())
                .expect("first install");
            assert_eq!(
                snapshot(&manager, query),
                Some((0, 0, lifecycle().delivery_expire, lifecycle().query_expire))
            );

            assert_eq!(
                manager
                    .abort_runtime_filter_deployment(query, install.epoch())
                    .expect("first abort"),
                RuntimeFilterDeploymentAbortOutcome::Applied
            );
            assert_eq!(snapshot(&manager, query), None);
            assert_eq!(
                manager
                    .abort_runtime_filter_deployment(query, install.epoch())
                    .expect("duplicate abort"),
                RuntimeFilterDeploymentAbortOutcome::Idempotent
            );
            assert_eq!(snapshot(&manager, query), None);
        }

        #[test]
        fn aborted_query_cannot_be_reinstalled_or_revived_by_envelope() {
            let manager = QueryContextManager::new_for_test();
            let query = query_id(9205);
            let install = participant_install();
            assert_eq!(snapshot(&manager, query), None);
            manager
                .install_runtime_filter_deployment(query, lifecycle(), install.clone())
                .expect("first install");
            assert_eq!(
                snapshot(&manager, query),
                Some((0, 0, lifecycle().delivery_expire, lifecycle().query_expire))
            );
            manager
                .abort_runtime_filter_deployment(query, install.epoch())
                .expect("abort");
            assert_eq!(snapshot(&manager, query), None);

            let error = manager
                .install_runtime_filter_deployment(query, lifecycle(), install)
                .expect_err("aborted query cannot be reinstalled");
            assert_eq!(
                error.kind(),
                RuntimeFilterDeploymentInstallErrorKind::QueryAborted
            );
            assert!(manager.runtime_filter_service_for_ingress(query).is_none());
            assert_eq!(snapshot(&manager, query), None);
        }

        #[test]
        fn installing_claim_rejects_fragment_registration_without_counter_mutation() {
            let manager = QueryContextManager::new_for_test();
            let query = query_id(9206);
            let (install_entered_tx, install_entered_rx) = mpsc::sync_channel(0);
            let (release_install_tx, release_install_rx) = mpsc::sync_channel(0);
            let release_install_rx = Mutex::new(release_install_rx);
            manager.set_before_runtime_filter_service_install_hook_for_test(Arc::new(move || {
                install_entered_tx.send(()).expect("signal installing");
                release_install_rx
                    .lock()
                    .expect("release install lock")
                    .recv()
                    .expect("release service install");
            }));

            let install_manager = manager.clone();
            let install = std::thread::spawn(move || {
                install_manager.install_runtime_filter_deployment(
                    query,
                    lifecycle(),
                    participant_install(),
                )
            });
            install_entered_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("install reached pre-Service barrier");

            assert!(manager.runtime_filter_deployment_is_installing_for_test(query));
            assert_eq!(manager.fragment_counts_for_test(query), Some((0, 0)));
            let fragment_error = manager
                .get_or_register_native(
                    query,
                    false,
                    lifecycle().delivery_expire,
                    lifecycle().query_expire,
                )
                .expect_err("fragment cannot register against Installing deployment");
            assert!(fragment_error.contains("installing"), "{fragment_error}");
            assert_eq!(manager.fragment_counts_for_test(query), Some((0, 0)));

            release_install_tx
                .send(())
                .expect("release service install");
            assert_eq!(
                install.join().expect("install thread").expect("install"),
                RuntimeFilterDeploymentInstallOutcome::Applied
            );
            assert!(manager.runtime_filter_deployment_is_installed_for_test(query));
            manager
                .get_or_register_native(
                    query,
                    false,
                    lifecycle().delivery_expire,
                    lifecycle().query_expire,
                )
                .expect("fragment registers after Installed publish");
            assert_eq!(manager.fragment_counts_for_test(query), Some((1, 1)));
        }

        #[test]
        fn duplicate_abort_waits_for_shutdown_and_aborted_publish() {
            let manager = QueryContextManager::new_for_test();
            let query = query_id(9207);
            let install = participant_install();
            manager
                .install_runtime_filter_deployment(query, lifecycle(), install.clone())
                .expect("install");

            let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::sync_channel(0);
            let (publish_aborted_tx, publish_aborted_rx) = mpsc::sync_channel(0);
            let publish_aborted_rx = Mutex::new(publish_aborted_rx);
            manager.set_after_runtime_filter_service_shutdown_hook_for_test(Arc::new(move || {
                shutdown_complete_tx
                    .send(())
                    .expect("signal shutdown complete");
                publish_aborted_rx
                    .lock()
                    .expect("publish Aborted lock")
                    .recv()
                    .expect("release Aborted publish");
            }));
            let (duplicate_waiting_tx, duplicate_waiting_rx) = mpsc::sync_channel(0);
            manager.set_abort_wait_observer_for_test(Arc::new(move || {
                duplicate_waiting_tx
                    .send(())
                    .expect("signal duplicate waits");
            }));

            let first_manager = manager.clone();
            let epoch = install.epoch();
            let first = std::thread::spawn(move || {
                first_manager.abort_runtime_filter_deployment(query, epoch)
            });
            shutdown_complete_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("first abort completed shutdown");
            assert!(manager.runtime_filter_deployment_is_aborting_for_test(query));

            let duplicate_manager = manager.clone();
            let (duplicate_result_tx, duplicate_result_rx) = mpsc::sync_channel(1);
            let duplicate = std::thread::spawn(move || {
                duplicate_result_tx
                    .send(duplicate_manager.abort_runtime_filter_deployment(query, epoch))
                    .expect("send duplicate result");
            });
            duplicate_waiting_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("duplicate observed Aborting");
            assert!(matches!(
                duplicate_result_rx.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));

            publish_aborted_tx.send(()).expect("publish Aborted");
            assert_eq!(
                first.join().expect("first abort").expect("first success"),
                RuntimeFilterDeploymentAbortOutcome::Applied
            );
            assert_eq!(
                duplicate_result_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("duplicate result")
                    .expect("duplicate success"),
                RuntimeFilterDeploymentAbortOutcome::Idempotent
            );
            duplicate.join().expect("duplicate abort thread");
            assert!(manager.runtime_filter_deployment_is_aborted_for_test(query));
        }

        #[test]
        fn terminal_record_blocks_revival_until_retry_horizon_then_cleanup_reclaims_it() {
            let manager = QueryContextManager::new_for_test();
            let query = query_id(9208);
            let install = participant_install();
            manager
                .install_runtime_filter_deployment(query, lifecycle(), install.clone())
                .expect("install");
            manager
                .abort_runtime_filter_deployment(query, install.epoch())
                .expect("abort");
            assert_eq!(manager.runtime_filter_terminal_count_for_test(), 1);

            let horizon = manager.runtime_filter_control_retry_horizon_for_test();
            manager
                .advance_runtime_filter_terminal_clock_for_test(horizon - Duration::from_millis(1));
            manager.clean_expired_for_test();
            assert_eq!(manager.runtime_filter_terminal_count_for_test(), 1);
            assert_eq!(
                manager
                    .install_runtime_filter_deployment(query, lifecycle(), participant_install(),)
                    .expect_err("install blocked inside retry horizon")
                    .kind(),
                RuntimeFilterDeploymentInstallErrorKind::QueryAborted
            );
            assert!(
                manager
                    .get_or_register_native(
                        query,
                        false,
                        lifecycle().delivery_expire,
                        lifecycle().query_expire,
                    )
                    .is_err()
            );
            assert!(manager.runtime_filter_service_for_ingress(query).is_none());

            manager.advance_runtime_filter_terminal_clock_for_test(Duration::from_millis(2));
            manager.clean_expired_for_test();
            assert_eq!(manager.runtime_filter_terminal_count_for_test(), 0);
            assert_eq!(
                manager
                    .install_runtime_filter_deployment(query, lifecycle(), participant_install())
                    .expect("install may reserve a fresh record after retry horizon"),
                RuntimeFilterDeploymentInstallOutcome::Applied
            );
        }

        #[test]
        fn terminal_capacity_fails_closed_without_evicting_live_record() {
            let manager = QueryContextManager::new_for_test();
            manager.set_runtime_filter_terminal_capacity_for_test(1);
            let first = query_id(9209);
            let second = query_id(9210);
            let epoch = participant_install().epoch();
            assert_eq!(
                manager
                    .abort_runtime_filter_deployment(first, epoch)
                    .expect("first terminal reservation"),
                RuntimeFilterDeploymentAbortOutcome::Applied
            );

            let capacity_error = manager
                .abort_runtime_filter_deployment(second, epoch)
                .expect_err("capacity must reject instead of evicting first terminal");
            assert_eq!(
                capacity_error.kind(),
                RuntimeFilterDeploymentInstallErrorKind::TerminalCapacity
            );
            assert_eq!(manager.runtime_filter_terminal_count_for_test(), 1);
            assert!(manager.runtime_filter_deployment_is_aborted_for_test(first));
            assert_eq!(
                manager
                    .install_runtime_filter_deployment(first, lifecycle(), participant_install())
                    .expect_err("live terminal must continue blocking its query")
                    .kind(),
                RuntimeFilterDeploymentInstallErrorKind::QueryAborted
            );
            assert_eq!(
                manager
                    .install_runtime_filter_deployment(second, lifecycle(), participant_install())
                    .expect_err("install must also fail closed at capacity")
                    .kind(),
                RuntimeFilterDeploymentInstallErrorKind::TerminalCapacity
            );
            assert!(
                manager
                    .get_or_register_native(
                        first,
                        false,
                        lifecycle().delivery_expire,
                        lifecycle().query_expire,
                    )
                    .is_err()
            );
            assert_eq!(manager.runtime_filter_terminal_count_for_test(), 1);
        }

        #[test]
        fn second_chance_validation_and_retry_preserve_location_and_deadlines() {
            let manager = QueryContextManager::new_for_test();
            let late = query_id(9211);
            manager
                .get_or_register_native(
                    late,
                    false,
                    lifecycle().delivery_expire,
                    lifecycle().query_expire,
                )
                .expect("late fragment context");
            manager.finish_fragment(late);
            let late_before = location_snapshot(&manager, late).expect("second chance");
            assert_eq!(late_before.0, ContextLocation::SecondChance);
            assert_eq!(
                manager
                    .install_runtime_filter_deployment(late, lifecycle(), participant_install())
                    .expect_err("first install after fragment completion is late")
                    .kind(),
                RuntimeFilterDeploymentInstallErrorKind::ActiveFragments
            );
            assert_eq!(location_snapshot(&manager, late), Some(late_before));

            let retry = query_id(9212);
            let install = participant_install();
            manager
                .install_runtime_filter_deployment(retry, lifecycle(), install.clone())
                .expect("initial install");
            manager
                .get_or_register_native(
                    retry,
                    false,
                    lifecycle().delivery_expire,
                    lifecycle().query_expire,
                )
                .expect("fragment");
            manager.finish_fragment(retry);
            let retry_before = location_snapshot(&manager, retry).expect("second chance");
            assert_eq!(retry_before.0, ContextLocation::SecondChance);

            assert_eq!(
                manager
                    .install_runtime_filter_deployment(retry, lifecycle(), install.clone())
                    .expect("identical retry"),
                RuntimeFilterDeploymentInstallOutcome::Idempotent
            );
            assert_eq!(location_snapshot(&manager, retry), Some(retry_before));
            assert_eq!(
                manager
                    .install_runtime_filter_deployment(
                        retry,
                        lifecycle(),
                        with_epoch(install.clone(), 5),
                    )
                    .expect_err("stale retry")
                    .kind(),
                RuntimeFilterDeploymentInstallErrorKind::StaleEpoch
            );
            assert_eq!(location_snapshot(&manager, retry), Some(retry_before));
            let mut conflicting_lifecycle = lifecycle();
            conflicting_lifecycle.query_expire += Duration::from_secs(1);
            assert_eq!(
                manager
                    .install_runtime_filter_deployment(retry, conflicting_lifecycle, install)
                    .expect_err("conflicting retry")
                    .kind(),
                RuntimeFilterDeploymentInstallErrorKind::ConflictingDeployment
            );
            assert_eq!(location_snapshot(&manager, retry), Some(retry_before));
        }

        #[test]
        fn capacity_guard_covers_existing_active_and_second_chance_empty_contexts() {
            for (offset, location) in [ContextLocation::Active, ContextLocation::SecondChance]
                .into_iter()
                .enumerate()
            {
                let manager = QueryContextManager::new_for_test();
                manager.set_runtime_filter_terminal_capacity_for_test(1);
                let target = query_id(9_220 + offset as i64 * 2);
                let occupied = query_id(9_221 + offset as i64 * 2);
                create_empty_context(&manager, target, location);
                let before = deployment_context_snapshot(&manager, target).expect("target context");
                assert_eq!(before.0, location);
                assert_eq!(before.1, 0);
                assert_eq!(before.2, 0);
                assert_eq!(before.5, None);

                let occupied_install = participant_install();
                manager
                    .install_runtime_filter_deployment(
                        occupied,
                        lifecycle(),
                        occupied_install.clone(),
                    )
                    .expect("occupy sole deployment record");
                assert_eq!(
                    manager
                        .install_runtime_filter_deployment(
                            target,
                            lifecycle(),
                            participant_install(),
                        )
                        .expect_err("existing empty context cannot bypass capacity")
                        .kind(),
                    RuntimeFilterDeploymentInstallErrorKind::TerminalCapacity
                );
                assert_eq!(deployment_context_snapshot(&manager, target), Some(before));

                manager
                    .abort_runtime_filter_deployment(occupied, occupied_install.epoch())
                    .expect("terminalize occupied record");
                let horizon = manager.runtime_filter_control_retry_horizon_for_test();
                manager.advance_runtime_filter_terminal_clock_for_test(
                    horizon + Duration::from_millis(1),
                );
                manager.clean_expired_for_test();
                assert_eq!(manager.runtime_filter_terminal_count_for_test(), 0);
                assert_eq!(
                    manager
                        .install_runtime_filter_deployment(
                            target,
                            lifecycle(),
                            participant_install(),
                        )
                        .expect("capacity recovers after terminal expiry"),
                    RuntimeFilterDeploymentInstallOutcome::Applied
                );
            }
        }

        #[test]
        fn cleanup_preserves_installing_context_and_identical_retry_waits_for_publish() {
            let manager = QueryContextManager::new_for_test();
            let query = query_id(9_224);
            let install = participant_install();
            let mut expiring_lifecycle = lifecycle();
            expiring_lifecycle.delivery_expire = Duration::ZERO;
            expiring_lifecycle.query_expire = Duration::ZERO;

            let (service_installed_tx, service_installed_rx) = mpsc::sync_channel(0);
            let (publish_release_tx, publish_release_rx) = mpsc::sync_channel(0);
            let publish_release_rx = Mutex::new(publish_release_rx);
            manager
                .inner
                .lock()
                .expect("query manager")
                .before_runtime_filter_installed_publish = Some(Arc::new(move || {
                service_installed_tx
                    .send(())
                    .expect("signal Service install complete");
                publish_release_rx
                    .lock()
                    .expect("publish release lock")
                    .recv()
                    .expect("release Installed publish");
            }));

            let first_manager = manager.clone();
            let first_install = install.clone();
            let first = std::thread::spawn(move || {
                first_manager.install_runtime_filter_deployment(
                    query,
                    expiring_lifecycle,
                    first_install,
                )
            });
            service_installed_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("Service installed before final publish");
            let before_cleanup =
                deployment_context_snapshot(&manager, query).expect("Installing context");
            assert_eq!(before_cleanup.0, ContextLocation::Active);
            assert_eq!(
                before_cleanup.5,
                Some(RuntimeFilterDeploymentClaimState::Installing)
            );

            manager.clean_expired_for_test();
            assert_eq!(
                deployment_context_snapshot(&manager, query),
                Some(before_cleanup)
            );

            let (retry_waiting_tx, retry_waiting_rx) = mpsc::sync_channel(0);
            let (retry_service_entered_tx, retry_service_entered_rx) = mpsc::sync_channel(1);
            {
                let mut guard = manager.inner.lock().expect("query manager");
                guard.install_wait_observer = Some(Arc::new(move || {
                    retry_waiting_tx.send(()).expect("signal retry wait");
                }));
                guard.before_runtime_filter_service_install = Some(Arc::new(move || {
                    retry_service_entered_tx
                        .send(())
                        .expect("signal duplicate Service install");
                }));
            }
            let retry_manager = manager.clone();
            let retry_install = install.clone();
            let (retry_result_tx, retry_result_rx) = mpsc::sync_channel(1);
            let retry = std::thread::spawn(move || {
                retry_result_tx
                    .send(retry_manager.install_runtime_filter_deployment(
                        query,
                        expiring_lifecycle,
                        retry_install,
                    ))
                    .expect("send retry result");
            });
            retry_waiting_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("identical retry observed Installing");
            assert!(matches!(
                retry_service_entered_rx.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));
            assert!(matches!(
                retry_result_rx.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));

            publish_release_tx.send(()).expect("publish Installed");
            assert_eq!(
                first.join().expect("first install").expect("first result"),
                RuntimeFilterDeploymentInstallOutcome::Applied
            );
            assert_eq!(
                retry_result_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("retry result")
                    .expect("retry success"),
                RuntimeFilterDeploymentInstallOutcome::Idempotent
            );
            retry.join().expect("retry thread");
            assert!(matches!(
                retry_service_entered_rx.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));
            assert!(manager.runtime_filter_deployment_is_installed_for_test(query));
        }

        #[test]
        fn second_chance_completion_cannot_remove_installing_context() {
            let manager = QueryContextManager::new_for_test();
            let query = query_id(9_229);
            let (service_installed_tx, service_installed_rx) = mpsc::sync_channel(0);
            let (publish_release_tx, publish_release_rx) = mpsc::sync_channel(0);
            let publish_release_rx = Mutex::new(publish_release_rx);
            manager
                .inner
                .lock()
                .expect("query manager")
                .before_runtime_filter_installed_publish = Some(Arc::new(move || {
                service_installed_tx
                    .send(())
                    .expect("signal Service install complete");
                publish_release_rx
                    .lock()
                    .expect("publish release lock")
                    .recv()
                    .expect("release Installed publish");
            }));

            let install_manager = manager.clone();
            let install = std::thread::spawn(move || {
                install_manager.install_runtime_filter_deployment(
                    query,
                    lifecycle(),
                    participant_install(),
                )
            });
            service_installed_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("Service installed before final publish");
            {
                let mut guard = manager.inner.lock().expect("query manager");
                let mut context = guard.active.remove(&query).expect("Installing context");
                context
                    .lookup_fetchers
                    .insert(42, LookupFetcherLifecycle::Exact(1));
                context.cancelled_by_fe = true;
                guard.second_chance.insert(query, context);
            }
            let before = deployment_context_snapshot(&manager, query);
            assert_eq!(
                before.as_ref().map(|snapshot| snapshot.0),
                Some(ContextLocation::SecondChance)
            );
            assert_eq!(
                before.as_ref().map(|snapshot| snapshot.5),
                Some(Some(RuntimeFilterDeploymentClaimState::Installing))
            );

            manager
                .complete_lookup_fetcher(query, 42)
                .expect("complete final lookup fetcher");
            assert_eq!(deployment_context_snapshot(&manager, query), before);

            publish_release_tx.send(()).expect("publish Installed");
            assert_eq!(
                install
                    .join()
                    .expect("install thread")
                    .expect_err("cancelled context cannot publish Installed")
                    .kind(),
                RuntimeFilterDeploymentInstallErrorKind::QueryAborted
            );
            assert!(manager.runtime_filter_deployment_is_aborted_for_test(query));
        }

        #[test]
        fn missing_context_before_installed_publish_terminalizes_captured_service() {
            let manager = QueryContextManager::new_for_test();
            let query = query_id(9_225);
            let hook_manager = manager.clone();
            manager
                .inner
                .lock()
                .expect("query manager")
                .before_runtime_filter_installed_publish = Some(Arc::new(move || {
                let removed = hook_manager
                    .inner
                    .lock()
                    .expect("query manager")
                    .active
                    .remove(&query);
                assert!(removed.is_some(), "remove captured Installing context");
            }));

            assert_eq!(
                manager
                    .install_runtime_filter_deployment(query, lifecycle(), participant_install())
                    .expect_err("missing final context rejects ACK")
                    .kind(),
                RuntimeFilterDeploymentInstallErrorKind::QueryAborted
            );
            assert!(manager.runtime_filter_deployment_is_aborted_for_test(query));
            assert!(manager.runtime_filter_service_for_ingress(query).is_none());
        }

        #[test]
        fn install_panic_terminalizes_instead_of_leaving_installing_claim() {
            let manager = QueryContextManager::new_for_test();
            let query = query_id(9_226);
            let mut invalid_lifecycle = lifecycle();
            invalid_lifecycle.transport_max_attempts = 0;

            let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = manager.install_runtime_filter_deployment(
                    query,
                    invalid_lifecycle,
                    participant_install(),
                );
            }));
            assert!(unwind.is_err(), "invalid transport policy still panics");
            assert!(!manager.runtime_filter_deployment_is_installing_for_test(query));
            assert!(manager.runtime_filter_deployment_is_aborted_for_test(query));
            assert!(manager.runtime_filter_service_for_ingress(query).is_none());
        }

        #[test]
        fn abort_hook_panic_publishes_aborted_and_wakes_duplicate() {
            let manager = QueryContextManager::new_for_test();
            let query = query_id(9_227);
            let install = participant_install();
            manager
                .install_runtime_filter_deployment(query, lifecycle(), install.clone())
                .expect("install");

            let (shutdown_reached_tx, shutdown_reached_rx) = mpsc::sync_channel(0);
            let (panic_release_tx, panic_release_rx) = mpsc::sync_channel(0);
            let panic_release_rx = Mutex::new(panic_release_rx);
            manager.set_after_runtime_filter_service_shutdown_hook_for_test(Arc::new(move || {
                shutdown_reached_tx
                    .send(())
                    .expect("signal shutdown reached");
                panic_release_rx
                    .lock()
                    .expect("panic release lock")
                    .recv()
                    .expect("release panic");
                panic!("abort hook panic");
            }));
            let (duplicate_waiting_tx, duplicate_waiting_rx) = mpsc::sync_channel(0);
            manager.set_abort_wait_observer_for_test(Arc::new(move || {
                duplicate_waiting_tx
                    .send(())
                    .expect("signal duplicate waits");
            }));

            let epoch = install.epoch();
            let first_manager = manager.clone();
            let (first_result_tx, first_result_rx) = mpsc::sync_channel(1);
            let first = std::thread::spawn(move || {
                first_result_tx
                    .send(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                        || first_manager.abort_runtime_filter_deployment(query, epoch),
                    )))
                    .expect("send first unwind");
            });
            shutdown_reached_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("first abort reached post-shutdown hook");
            assert!(manager.runtime_filter_deployment_is_aborting_for_test(query));

            let duplicate_manager = manager.clone();
            let (duplicate_result_tx, duplicate_result_rx) = mpsc::sync_channel(1);
            let duplicate = std::thread::spawn(move || {
                duplicate_result_tx
                    .send(duplicate_manager.abort_runtime_filter_deployment(query, epoch))
                    .expect("send duplicate result");
            });
            duplicate_waiting_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("duplicate observed Aborting");
            panic_release_tx.send(()).expect("release abort hook panic");
            assert!(
                first_result_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("first unwind result")
                    .is_err()
            );
            first.join().expect("first abort thread");
            assert_eq!(
                duplicate_result_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("duplicate result")
                    .expect("duplicate success"),
                RuntimeFilterDeploymentAbortOutcome::Idempotent
            );
            duplicate.join().expect("duplicate abort thread");
            assert!(manager.runtime_filter_deployment_is_aborted_for_test(query));

            let horizon = manager.runtime_filter_control_retry_horizon_for_test();
            manager
                .advance_runtime_filter_terminal_clock_for_test(horizon + Duration::from_millis(1));
            manager.clean_expired_for_test();
            assert_eq!(manager.runtime_filter_terminal_count_for_test(), 0);
        }

        #[test]
        fn install_against_terminal_classifies_request_epoch() {
            let manager = QueryContextManager::new_for_test();
            let query = query_id(9_228);
            let terminal_epoch = DeploymentEpoch::new(10);
            manager
                .abort_runtime_filter_deployment(query, terminal_epoch)
                .expect("create terminal");

            for (epoch, expected) in [
                (10, RuntimeFilterDeploymentInstallErrorKind::QueryAborted),
                (5, RuntimeFilterDeploymentInstallErrorKind::StaleEpoch),
                (
                    20,
                    RuntimeFilterDeploymentInstallErrorKind::ConflictingDeployment,
                ),
            ] {
                assert_eq!(
                    manager
                        .install_runtime_filter_deployment(
                            query,
                            lifecycle(),
                            with_epoch(participant_install(), epoch),
                        )
                        .expect_err("terminal rejects install")
                        .kind(),
                    expected
                );
            }
        }

        #[test]
        fn cancel_query_during_installed_publish_rejects_ack_and_revival() {
            let manager = QueryContextManager::new_for_test();
            let query = query_id(9_230);
            let install = participant_install();
            let (service_installed_tx, service_installed_rx) = mpsc::sync_channel(0);
            let (publish_release_tx, publish_release_rx) = mpsc::sync_channel(0);
            let publish_release_rx = Mutex::new(publish_release_rx);
            manager
                .inner
                .lock()
                .expect("query manager")
                .before_runtime_filter_installed_publish = Some(Arc::new(move || {
                service_installed_tx
                    .send(())
                    .expect("signal Service install complete");
                publish_release_rx
                    .lock()
                    .expect("publish release lock")
                    .recv()
                    .expect("release Installed publish");
            }));

            let install_manager = manager.clone();
            let first_install = install.clone();
            let installer = std::thread::spawn(move || {
                install_manager.install_runtime_filter_deployment(query, lifecycle(), first_install)
            });
            service_installed_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("Service installed before final publish");
            assert_eq!(manager.fragment_counts_for_test(query), Some((0, 0)));

            assert!(
                manager
                    .cancel_query(query, "query cancelled".to_string())
                    .is_empty()
            );
            assert!(manager.runtime_filter_deployment_is_aborted_for_test(query));
            assert_eq!(manager.fragment_counts_for_test(query), Some((0, 0)));

            publish_release_tx
                .send(())
                .expect("release Installed publish");
            assert_eq!(
                installer
                    .join()
                    .expect("installer thread")
                    .expect_err("cancelled install cannot ACK")
                    .kind(),
                RuntimeFilterDeploymentInstallErrorKind::QueryAborted
            );
            assert_eq!(
                manager
                    .install_runtime_filter_deployment(query, lifecycle(), install)
                    .expect_err("cancelled query cannot be reinstalled")
                    .kind(),
                RuntimeFilterDeploymentInstallErrorKind::QueryAborted
            );
            assert!(
                manager
                    .get_or_register_native(
                        query,
                        false,
                        lifecycle().delivery_expire,
                        lifecycle().query_expire,
                    )
                    .is_err()
            );
            assert_eq!(manager.fragment_counts_for_test(query), Some((0, 0)));
        }

        #[test]
        fn cancel_after_installed_rejects_duplicate_install_and_fragment_registration() {
            let manager = QueryContextManager::new_for_test();
            let query = query_id(9_231);
            let install = participant_install();
            manager
                .install_runtime_filter_deployment(query, lifecycle(), install.clone())
                .expect("install");
            assert!(manager.runtime_filter_deployment_is_installed_for_test(query));

            let primary_error = "fragment execution error: bitmap aggregate expects INTEGER input";
            manager.cancel_query(query, primary_error.to_string());

            assert!(manager.runtime_filter_deployment_is_aborted_for_test(query));
            assert_eq!(
                manager
                    .install_runtime_filter_deployment(query, lifecycle(), install)
                    .expect_err("cancelled Installed deployment is not idempotent success")
                    .kind(),
                RuntimeFilterDeploymentInstallErrorKind::QueryAborted
            );
            manager.cancel_query(query, "secondary cancellation".to_string());
            assert_eq!(
                manager
                    .get_or_register_native(
                        query,
                        false,
                        lifecycle().delivery_expire,
                        lifecycle().query_expire,
                    )
                    .expect_err("cancelled query rejects late fragment registration"),
                primary_error
            );
            assert_eq!(manager.fragment_counts_for_test(query), Some((0, 0)));
        }

        #[test]
        fn abort_query_terminalizes_deployment_and_preserves_finst_result() {
            let manager = QueryContextManager::new_for_test();
            let query = query_id(9_232);
            let finst_a = UniqueId { hi: 9_233, lo: 1 };
            let finst_b = UniqueId { hi: 9_233, lo: 2 };
            manager
                .install_runtime_filter_deployment(query, lifecycle(), participant_install())
                .expect("install");
            manager.register_finsts([finst_a, finst_b], query);

            let finsts = manager.abort_query(query);

            assert_eq!(finsts.len(), 2);
            assert!(finsts.contains(&finst_a));
            assert!(finsts.contains(&finst_b));
            assert!(manager.runtime_filter_deployment_is_aborted_for_test(query));
        }

        #[test]
        fn cancel_query_execution_terminalizes_matching_deployment_and_preserves_finst_result() {
            let manager = QueryContextManager::new_for_test();
            let query = query_id(9_234);
            let finst = UniqueId { hi: 9_235, lo: 1 };
            manager
                .install_runtime_filter_deployment(query, lifecycle(), participant_install())
                .expect("install");
            manager.register_finst(finst, query);

            assert_eq!(
                manager.cancel_query_execution(
                    QueryExecutionKey::native(query),
                    "execution cancelled".to_string(),
                ),
                vec![finst]
            );
            assert!(manager.runtime_filter_deployment_is_aborted_for_test(query));
        }

        #[test]
        fn cancel_finst_observer_terminalizes_deployment_and_preserves_query_result() {
            let manager = QueryContextManager::new_for_test();
            let query = query_id(9_236);
            let finst_a = UniqueId { hi: 9_237, lo: 1 };
            let finst_b = UniqueId { hi: 9_237, lo: 2 };
            manager
                .install_runtime_filter_deployment(query, lifecycle(), participant_install())
                .expect("install");
            manager.register_finsts([finst_a, finst_b], query);
            let (observer_tx, observer_rx) = mpsc::sync_channel(1);

            let result =
                manager.cancel_finst_internal(finst_a, "finst cancelled".to_string(), || {
                    observer_tx.send(()).expect("binding observer")
                });

            observer_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("observer ran while binding was resolved");
            assert_eq!(result.query_id, Some(query));
            assert_eq!(result.finsts.len(), 2);
            assert!(result.finsts.contains(&finst_a));
            assert!(result.finsts.contains(&finst_b));
            assert!(manager.runtime_filter_deployment_is_aborted_for_test(query));
        }

        #[test]
        fn cancelled_empty_native_context_blocks_revival_until_retry_horizon() {
            let manager = QueryContextManager::new_for_test();
            let query = query_id(9_238);
            manager
                .ensure_native_context(
                    query,
                    false,
                    lifecycle().delivery_expire,
                    lifecycle().query_expire,
                )
                .expect("create empty native context");
            assert!(
                manager
                    .cancel_query(query, "cancel empty context".to_string())
                    .is_empty()
            );

            assert_eq!(
                manager
                    .install_runtime_filter_deployment(query, lifecycle(), participant_install())
                    .expect_err("query cancel tombstone blocks late install")
                    .kind(),
                RuntimeFilterDeploymentInstallErrorKind::QueryAborted
            );
            assert!(
                manager
                    .get_or_register_native(
                        query,
                        false,
                        lifecycle().delivery_expire,
                        lifecycle().query_expire,
                    )
                    .is_err()
            );

            {
                let mut guard = manager.inner.lock().expect("query manager");
                guard
                    .active
                    .get_mut(&query)
                    .expect("cancelled context remains active")
                    .query_deadline = Instant::now() - Duration::from_millis(1);
            }
            manager.clean_expired_for_test();
            assert_eq!(manager.fragment_counts_for_test(query), Some((0, 0)));

            let horizon = manager.runtime_filter_control_retry_horizon_for_test();
            manager
                .advance_runtime_filter_terminal_clock_for_test(horizon + Duration::from_millis(1));
            manager.clean_expired_for_test();
            assert_eq!(
                manager
                    .install_runtime_filter_deployment(query, lifecycle(), participant_install())
                    .expect("query id may be reused after cancellation retry horizon"),
                RuntimeFilterDeploymentInstallOutcome::Applied
            );
        }

        #[test]
        fn truly_contextless_cancel_preserves_baseline_noop_semantics() {
            let manager = QueryContextManager::new_for_test();
            let query = query_id(9_240);

            assert!(
                manager
                    .cancel_query(query, "unknown query".to_string())
                    .is_empty()
            );
            assert_eq!(
                manager
                    .install_runtime_filter_deployment(query, lifecycle(), participant_install(),)
                    .expect("unknown-query cancellation does not reserve an epochless record"),
                RuntimeFilterDeploymentInstallOutcome::Applied
            );
        }

        #[test]
        fn cancelled_empty_context_fails_closed_when_terminal_capacity_is_full() {
            let manager = QueryContextManager::new_for_test();
            manager.set_runtime_filter_terminal_capacity_for_test(1);
            let occupied = query_id(9_241);
            let cancelled = query_id(9_242);
            let unrelated = query_id(9_243);
            manager
                .ensure_native_context(
                    cancelled,
                    false,
                    lifecycle().delivery_expire,
                    lifecycle().query_expire,
                )
                .expect("create target empty context");
            manager
                .ensure_native_context(
                    unrelated,
                    false,
                    lifecycle().delivery_expire,
                    lifecycle().query_expire,
                )
                .expect("create unrelated empty context");
            manager
                .abort_runtime_filter_deployment(occupied, DeploymentEpoch::new(1))
                .expect("reserve the only terminal record");

            manager.cancel_query(cancelled, "cancel empty context".to_string());

            assert_eq!(
                manager
                    .install_runtime_filter_deployment(
                        cancelled,
                        lifecycle(),
                        participant_install(),
                    )
                    .expect_err("cancelled context itself must fail closed at capacity")
                    .kind(),
                RuntimeFilterDeploymentInstallErrorKind::QueryAborted
            );
            manager
                .get_or_register_native(
                    unrelated,
                    false,
                    lifecycle().delivery_expire,
                    lifecycle().query_expire,
                )
                .expect("unrelated existing native path is not globally blocked");
            let fresh = query_id(9_244);
            manager
                .get_or_register_native(
                    fresh,
                    false,
                    lifecycle().delivery_expire,
                    lifecycle().query_expire,
                )
                .expect("unrelated new native path is not globally blocked");

            {
                let mut guard = manager.inner.lock().expect("query manager");
                guard
                    .active
                    .get_mut(&cancelled)
                    .expect("cancelled context retained")
                    .query_deadline = Instant::now() - Duration::from_millis(1);
            }
            manager.clean_expired_for_test();
            assert_eq!(manager.fragment_counts_for_test(cancelled), Some((0, 0)));

            let horizon = manager.runtime_filter_control_retry_horizon_for_test();
            manager
                .advance_runtime_filter_terminal_clock_for_test(horizon + Duration::from_millis(1));
            manager.clean_expired_for_test();
            assert_eq!(
                manager
                    .install_runtime_filter_deployment(
                        cancelled,
                        lifecycle(),
                        participant_install(),
                    )
                    .expect("capacity and saturation are reclaimed together"),
                RuntimeFilterDeploymentInstallOutcome::Applied
            );
        }

        #[test]
        fn cancellation_races_explicit_abort_and_installer_without_double_terminalization() {
            let manager = QueryContextManager::new_for_test();
            let query = query_id(9_239);
            let install = participant_install();
            let epoch = install.epoch();
            let (service_installed_tx, service_installed_rx) = mpsc::sync_channel(0);
            let (install_release_tx, install_release_rx) = mpsc::sync_channel(0);
            let install_release_rx = Mutex::new(install_release_rx);
            manager
                .inner
                .lock()
                .expect("query manager")
                .before_runtime_filter_installed_publish = Some(Arc::new(move || {
                service_installed_tx.send(()).expect("Service installed");
                install_release_rx
                    .lock()
                    .expect("install release lock")
                    .recv()
                    .expect("release installer");
            }));
            let install_manager = manager.clone();
            let installer = std::thread::spawn(move || {
                install_manager.install_runtime_filter_deployment(query, lifecycle(), install)
            });
            service_installed_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("installer reached publish barrier");

            let (shutdown_reached_tx, shutdown_reached_rx) = mpsc::sync_channel(0);
            let (abort_publish_tx, abort_publish_rx) = mpsc::sync_channel(0);
            let abort_publish_rx = Mutex::new(abort_publish_rx);
            manager.set_after_runtime_filter_service_shutdown_hook_for_test(Arc::new(move || {
                shutdown_reached_tx.send(()).expect("shutdown reached");
                abort_publish_rx
                    .lock()
                    .expect("abort publish lock")
                    .recv()
                    .expect("publish Aborted");
            }));
            let cancel_manager = manager.clone();
            let cancel = std::thread::spawn(move || {
                cancel_manager.cancel_query(query, "query cancelled".to_string())
            });
            shutdown_reached_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("cancellation reached shutdown hook");
            assert!(manager.runtime_filter_deployment_is_aborting_for_test(query));

            let (abort_waiting_tx, abort_waiting_rx) = mpsc::sync_channel(0);
            manager.set_abort_wait_observer_for_test(Arc::new(move || {
                abort_waiting_tx.send(()).expect("abort waits");
            }));
            let abort_manager = manager.clone();
            let (abort_result_tx, abort_result_rx) = mpsc::sync_channel(1);
            let abort = std::thread::spawn(move || {
                abort_result_tx
                    .send(abort_manager.abort_runtime_filter_deployment(query, epoch))
                    .expect("send abort result");
            });
            abort_waiting_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("explicit abort observed cancellation Aborting");

            abort_publish_tx
                .send(())
                .expect("publish cancellation Aborted");
            assert!(cancel.join().expect("cancel thread").is_empty());
            assert_eq!(
                abort_result_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("abort result")
                    .expect("abort success"),
                RuntimeFilterDeploymentAbortOutcome::Idempotent
            );
            abort.join().expect("abort thread");
            install_release_tx.send(()).expect("release installer");
            assert_eq!(
                installer
                    .join()
                    .expect("installer thread")
                    .expect_err("cancelled install cannot ACK")
                    .kind(),
                RuntimeFilterDeploymentInstallErrorKind::QueryAborted
            );
            assert!(manager.runtime_filter_deployment_is_aborted_for_test(query));
            assert_eq!(manager.runtime_filter_terminal_count_for_test(), 1);
        }
    }
}
