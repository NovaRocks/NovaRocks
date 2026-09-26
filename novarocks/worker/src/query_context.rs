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

use novarocks_execution::runtime::fragment::io::ExchangeReceiverPort;
use novarocks_execution::runtime::mem_tracker::{self, MemTracker};
use novarocks_memory::{AccountHandle, AccountKind, ExternalRef, MemoryAuthority};
use novarocks_types::{QueryId, UniqueId};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum QueryExecutionGeneration {
    Native(NonZeroU64),
}

#[allow(
    dead_code,
    reason = "Legacy query-context callers retain the first native attempt outside the lifecycle-aware worker runtime."
)]
fn legacy_native_attempt() -> NonZeroU64 {
    NonZeroU64::new(1).expect("one is a nonzero native attempt")
}

// Design: ADR-0159 (docs/adr/ADR-0159-driver-polled-connector-scan-streams.md)
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct QueryExecutionKey {
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

    pub const fn native_attempt(query_id: QueryId, attempt: NonZeroU64) -> Self {
        Self {
            query_id,
            generation: QueryExecutionGeneration::Native(attempt),
        }
    }

    pub const fn query_id(self) -> QueryId {
        self.query_id
    }

    pub const fn native_attempt_id(self) -> Option<NonZeroU64> {
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

impl QueryContextGeneration {
    const fn execution_key(query_id: QueryId, generation: Self) -> QueryExecutionKey {
        match generation {
            Self::Native(attempt) => QueryExecutionKey::native_attempt(query_id, attempt),
        }
    }
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
    reason = "Query context fields are consumed by native execution integrations outside the worker test configuration."
)]
pub(crate) struct QueryContext {
    #[allow(dead_code)]
    pub(crate) query_id: QueryId,
    execution_generation: QueryContextGeneration,
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
    /// This query's memory account, created on first ask.
    ///
    /// The account lives exactly as long as the context does, which is what
    /// makes the query's capacity return on its own when the query ends. It
    /// stays `None` until an owner that was *handed* a memory authority asks
    /// for it: this registry is a process-global singleton and must not be a
    /// place capacity can be reached from.
    mem_account: Option<AccountHandle>,
    cleanup_leases: Vec<QueryCleanupLease>,
}

#[derive(Default)]
struct RuntimeFilterQueryCancellationAction;

#[allow(
    dead_code,
    reason = "Query context helpers are consumed by native execution integrations outside the worker test configuration."
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
        // Same parent and same label as every other query-tracker owner: the
        // label is minted once in the execution crate so the two construction
        // paths cannot drift apart.
        let query_label = mem_tracker::query_tracker_label(query_id.high(), query_id.low());
        let mem_tracker = MemTracker::new_child(query_label, &process);
        Self {
            query_id,
            execution_generation,
            num_fragments: 0,
            num_active_fragments: 0,
            total_fragments: None,
            cancelled_by_fe: false,
            delivery_expire,
            delivery_deadline: now + delivery_expire,
            query_expire,
            query_deadline: now + query_expire,
            mem_tracker,
            mem_account: None,
            cleanup_leases: Vec::new(),
        }
    }

    /// The execution this context belongs to: its query and attempt.
    fn execution_key(&self) -> QueryExecutionKey {
        QueryContextGeneration::execution_key(self.query_id, self.execution_generation)
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

/// Contexts are keyed by execution, not by query: an attempt that was
/// aborted keeps its own context while it drains, so a newer attempt of the
/// same query is admitted beside it instead of being mistaken for it.
#[derive(Default)]
struct QueryContextManagerInner {
    active: HashMap<QueryExecutionKey, QueryContext>,
    second_chance: HashMap<QueryExecutionKey, QueryContext>,
    finst_to_query: HashMap<UniqueId, QueryExecutionKey>,
    exchange_receiver_ports: HashMap<UniqueId, Arc<dyn ExchangeReceiverPort>>,
}

impl QueryContextManagerInner {
    fn context(&self, execution: QueryExecutionKey) -> Option<&QueryContext> {
        self.active
            .get(&execution)
            .or_else(|| self.second_chance.get(&execution))
    }

    fn context_mut(&mut self, execution: QueryExecutionKey) -> Option<&mut QueryContext> {
        if self.active.contains_key(&execution) {
            self.active.get_mut(&execution)
        } else {
            self.second_chance.get_mut(&execution)
        }
    }

    /// Every context of one query, of whichever attempt.
    fn query_contexts_mut(&mut self, query_id: QueryId) -> impl Iterator<Item = &mut QueryContext> {
        self.active
            .values_mut()
            .chain(self.second_chance.values_mut())
            .filter(move |context| context.query_id == query_id)
    }
}

pub struct QueryContextManager {
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
    reason = "Fragment cancellation results are retained for native worker integrations outside the worker test configuration."
)]
pub(crate) struct FinstCancelResult {
    pub(crate) query_id: Option<QueryId>,
    pub(crate) finsts: Vec<UniqueId>,
}

#[allow(
    dead_code,
    reason = "Query-context manager APIs are consumed by native worker integrations outside the worker test configuration."
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

    #[cfg(any(test, feature = "test-support"))]
    pub fn new_for_test() -> Arc<Self> {
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
                .filter_map(|(execution, ctx)| {
                    (ctx.has_no_active_instances() && ctx.is_delivery_expired())
                        .then_some(*execution)
                })
                .collect::<Vec<_>>();
            let expired_active = guard
                .active
                .iter()
                .filter_map(|(execution, ctx)| {
                    (ctx.has_no_active_instances() && ctx.is_query_expired()).then_some(*execution)
                })
                .collect::<Vec<_>>();
            let mut expired = Vec::with_capacity(
                expired_second_chance
                    .len()
                    .saturating_add(expired_active.len()),
            );
            expired.extend(expired_second_chance.into_iter().filter_map(|execution| {
                guard
                    .second_chance
                    .remove(&execution)
                    .map(|ctx| (execution, ctx))
            }));
            expired.extend(expired_active.into_iter().filter_map(|execution| {
                guard.active.remove(&execution).map(|ctx| (execution, ctx))
            }));
            expired
        };
        drop(expired);
    }

    #[cfg(test)]
    pub(crate) fn clean_expired_for_test(&self) {
        self.clean_expired();
    }

    #[cfg(test)]
    pub(crate) fn expire_delivery_for_test(&self, execution: QueryExecutionKey) {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        let context = guard
            .context_mut(execution)
            .expect("query context must exist");
        context.delivery_deadline = Instant::now() - Duration::from_millis(1);
    }

    #[cfg(test)]
    pub(crate) fn fragment_counts_for_test(
        &self,
        execution: QueryExecutionKey,
    ) -> Option<(usize, usize)> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .context(execution)
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

    /// Returns this query's memory account, creating it on first ask.
    ///
    /// The authority arrives as an argument rather than as registry state.
    /// This registry is a process-global singleton; letting it hold the
    /// authority would make "one authority per process" true by accident of
    /// that singleton rather than by composition, and would hand every caller
    /// a way to reach capacity without being given it.
    pub fn ensure_query_account(
        &self,
        execution: QueryExecutionKey,
        authority: &Arc<MemoryAuthority>,
    ) -> Result<AccountHandle, String> {
        let mut inner = self.inner.lock().expect("query context manager lock");
        let context = inner
            .context_mut(execution)
            .ok_or_else(|| "QueryContext missing for memory account".to_string())?;
        if let Some(account) = context.mem_account.as_ref() {
            return Ok(account.clone());
        }
        let query_id = execution.query_id();
        let account = authority
            .create_account(
                AccountKind::Work,
                ExternalRef::new(query_id.high() as u64, query_id.low() as u64),
            )
            .map_err(|error| format!("create query memory account: {error}"))?;
        context.mem_account = Some(account.clone());
        Ok(account)
    }

    pub fn ensure_native_context_execution(
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

    pub fn get_or_register_native_execution(
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

    /// Retires a preparation-only native context once its Task owner has
    /// closed admission and all tasks have physically converged. A live
    /// fragment or another attempt's context remains owned by its own path.
    pub fn retire_idle_native_execution(&self, execution: QueryExecutionKey) -> bool {
        if execution.native_attempt_id().is_none() {
            return false;
        }
        let removed = {
            let mut guard = self.inner.lock().expect("query_ctx_manager lock");
            let eligible = guard
                .active
                .get(&execution)
                .is_some_and(|context| context.num_active_fragments == 0)
                && !guard
                    .finst_to_query
                    .values()
                    .any(|current| *current == execution);
            eligible.then(|| {
                guard
                    .active
                    .remove(&execution)
                    .expect("checked idle context")
            })
        };
        let retired = removed.is_some();
        drop(removed);
        retired
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
        let execution = QueryContextGeneration::execution_key(query_id, generation);
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        if let Some(existing) = guard.active.get_mut(&execution) {
            if increment {
                existing.increment_num_fragments();
            }
            return Ok(());
        }
        if let Some(mut existing) = guard.second_chance.remove(&execution) {
            if increment {
                existing.increment_num_fragments();
            }
            guard.active.insert(execution, existing);
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
        guard.active.insert(execution, ctx);
        Ok(())
    }

    pub(crate) fn with_context_mut<T, F>(
        &self,
        execution: QueryExecutionKey,
        f: F,
    ) -> Result<T, String>
    where
        F: FnOnce(&mut QueryContext) -> Result<T, String>,
    {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        let ctx = guard
            .active
            .get_mut(&execution)
            .ok_or_else(|| "QueryContext not found".to_string())?;
        f(ctx)
    }

    pub(crate) fn attach_cleanup_lease(
        &self,
        execution: QueryExecutionKey,
        lease: QueryCleanupLease,
    ) -> Result<(), String> {
        self.with_context_mut(execution, |ctx| {
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
            .map(|execution| execution.query_id())
            .collect::<Vec<_>>();
        query_ids.sort_by_key(|query_id| (query_id.high(), query_id.low()));
        query_ids.dedup();
        query_ids
    }

    /// Returns one execution's query tracker, for its admission and for
    /// lifecycle verification.
    pub fn query_mem_tracker_execution(
        &self,
        execution: QueryExecutionKey,
    ) -> Option<Arc<MemTracker>> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard.context(execution).map(QueryContext::mem_tracker)
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

    pub fn register_native_finst_execution(
        &self,
        finst_id: UniqueId,
        execution: QueryExecutionKey,
    ) -> Result<(), String> {
        if execution.native_attempt_id().is_none() {
            return Err("native finst registration requires a native execution key".to_string());
        }
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        if !guard.active.contains_key(&execution) {
            return Err("QueryContext not found".to_string());
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
        self.rollback_pre_ready(QueryExecutionKey::native(query_id), finst_id, true)
    }

    pub fn rollback_pre_ready_native_fragment_execution(
        &self,
        execution: QueryExecutionKey,
        finst_id: UniqueId,
    ) -> bool {
        self.rollback_pre_ready(execution, finst_id, false)
    }

    fn rollback_pre_ready(
        &self,
        execution: QueryExecutionKey,
        finst_id: UniqueId,
        release_exchange_port: bool,
    ) -> bool {
        let removed = {
            let mut guard = self.inner.lock().expect("query_ctx_manager lock");
            if guard.finst_to_query.get(&finst_id) != Some(&execution) {
                return false;
            }
            let Some(context) = guard.active.get(&execution) else {
                return false;
            };
            if context.num_fragments == 0 || context.num_active_fragments == 0 {
                return false;
            }
            guard.finst_to_query.remove(&finst_id);
            if release_exchange_port {
                guard.exchange_receiver_ports.remove(&finst_id);
            }
            let remove_empty_context = {
                let context = guard
                    .active
                    .get_mut(&execution)
                    .expect("checked active context");
                context.rollback_inc_fragments();
                context.num_fragments == 0 && context.num_active_fragments == 0
            };
            remove_empty_context.then(|| {
                guard
                    .active
                    .remove(&execution)
                    .expect("checked empty active context")
            })
        };
        drop(removed);
        true
    }

    pub fn unregister_finst_execution(&self, finst_id: UniqueId, execution: QueryExecutionKey) {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        if guard.finst_to_query.get(&finst_id) != Some(&execution) {
            return;
        }
        guard.finst_to_query.remove(&finst_id);
        guard.exchange_receiver_ports.remove(&finst_id);
    }

    pub(crate) fn get_query_timeout_by_finst(&self, finst_id: UniqueId) -> Option<Duration> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        let execution = *guard.finst_to_query.get(&finst_id)?;
        guard.context(execution).map(|ctx| ctx.query_expire)
    }

    #[cfg(test)]
    pub(crate) fn is_execution_canceled_for_test(&self, execution: QueryExecutionKey) -> bool {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .context(execution)
            .is_some_and(|ctx| ctx.cancelled_by_fe)
    }

    /// Marks the cancelled contexts: one execution's, or every attempt of the
    /// query when none is named.
    fn prepare_runtime_filter_query_cancellation(
        inner: &mut QueryContextManagerInner,
        query_id: QueryId,
        expected_execution: Option<QueryExecutionKey>,
        _cancellation_error: Option<&str>,
    ) -> RuntimeFilterQueryCancellationAction {
        match expected_execution {
            Some(execution) => {
                if let Some(context) = inner.context_mut(execution) {
                    context.cancelled_by_fe = true;
                }
            }
            None => {
                for context in inner.query_contexts_mut(query_id) {
                    context.cancelled_by_fe = true;
                }
            }
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
                && guard
                    .active
                    .get(&execution)
                    .is_some_and(|context| context.num_active_fragments == 0))
            .then(|| guard.active.remove(&execution))
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

    pub fn finish_fragment_execution(&self, execution: QueryExecutionKey) {
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
        let key = execution.unwrap_or_else(|| QueryExecutionKey::native(query_id));
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        let Some(mut ctx) = guard.active.remove(&key) else {
            return;
        };
        let no_active_fragments = ctx.count_down_fragments();
        if !no_active_fragments {
            guard.active.insert(key, ctx);
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
        guard.second_chance.insert(key, ctx);
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

#[cfg(test)]
mod attempt_isolation_tests {
    use std::num::NonZeroU64;
    use std::sync::Arc;
    use std::time::Duration;

    use novarocks_types::UniqueId;

    use super::{QueryContextManager, QueryExecutionKey, QueryId};

    fn attempt(query_id: QueryId, attempt: u64) -> QueryExecutionKey {
        QueryExecutionKey::native_attempt(query_id, NonZeroU64::new(attempt).expect("attempt"))
    }

    fn admit_fragment(manager: &QueryContextManager, execution: QueryExecutionKey) {
        manager
            .get_or_register_native_execution(
                execution,
                false,
                Duration::from_secs(1),
                Duration::from_secs(5),
            )
            .expect("admit a fragment");
    }

    /// An aborted attempt can still be draining a fragment -- a driver inside
    /// a kernel that cannot be interrupted -- when the frontend replans. The
    /// replacement attempt must be admitted beside it, with its own tracker,
    /// instead of being mistaken for the attempt it replaces.
    #[test]
    fn a_newer_attempt_is_admitted_beside_an_older_one_still_draining() {
        let manager = QueryContextManager::new_for_test();
        let query_id = QueryId::new(7_301, 7_302);
        let (first, second) = (attempt(query_id, 1), attempt(query_id, 2));
        admit_fragment(&manager, first);

        manager
            .ensure_native_context_execution(
                second,
                false,
                Duration::from_secs(1),
                Duration::from_secs(5),
            )
            .expect("the replacement attempt is admitted");
        let first_tracker = manager
            .query_mem_tracker_execution(first)
            .expect("the draining attempt keeps its context");
        let second_tracker = manager
            .query_mem_tracker_execution(second)
            .expect("the replacement attempt has its own context");
        assert!(!Arc::ptr_eq(&first_tracker, &second_tracker));
        assert_eq!(
            manager.native_execution_resource_snapshot().active_contexts,
            2
        );

        // The old attempt's fragment ends; only its own context goes.
        manager.finish_fragment_execution(first);
        assert!(manager.query_mem_tracker_execution(first).is_none());
        assert!(manager.query_mem_tracker_execution(second).is_some());
        assert_eq!(
            manager.native_execution_resource_snapshot().active_contexts,
            1
        );
    }

    #[test]
    fn cancelling_one_attempt_leaves_the_other_and_a_query_cancel_reaches_both() {
        let manager = QueryContextManager::new_for_test();
        let query_id = QueryId::new(7_311, 7_312);
        let (first, second) = (attempt(query_id, 1), attempt(query_id, 2));
        let (finst_first, finst_second) = (UniqueId::new(7_313, 1), UniqueId::new(7_313, 2));
        admit_fragment(&manager, first);
        admit_fragment(&manager, second);
        manager
            .register_native_finst_execution(finst_first, first)
            .expect("register the first attempt's fragment");
        manager
            .register_native_finst_execution(finst_second, second)
            .expect("register the second attempt's fragment");

        assert_eq!(
            manager.cancel_query_execution(first, "abort the first attempt".to_string()),
            vec![finst_first]
        );
        assert!(manager.is_execution_canceled_for_test(first));
        assert!(!manager.is_execution_canceled_for_test(second));

        let mut routed = manager.cancel_query(query_id, "kill the query".to_string());
        routed.sort_by_key(|finst| (finst.high(), finst.low()));
        assert_eq!(routed, vec![finst_first, finst_second]);
        assert!(manager.is_execution_canceled_for_test(second));
    }
}

static QUERY_CONTEXT_MANAGER: OnceLock<Arc<QueryContextManager>> = OnceLock::new();

pub fn query_context_manager() -> Arc<QueryContextManager> {
    QUERY_CONTEXT_MANAGER
        .get_or_init(QueryContextManager::new)
        .clone()
}

#[cfg(test)]
mod sender_error_tests {
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    use super::{QueryContextManager, QueryContextManagerInner, QueryExecutionKey, QueryId};
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
        assert!(mgr.is_execution_canceled_for_test(QueryExecutionKey::native(qid)));
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
    use std::num::NonZeroU64;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    use super::{
        QueryCleanupLease, QueryContextManager, QueryContextManagerInner, QueryExecutionKey,
        QueryId,
    };
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

        let execution = QueryExecutionKey::native(query_id);
        assert!(mgr.rollback_pre_ready_native_fragment(query_id, second));
        assert_eq!(mgr.fragment_counts_for_test(execution), Some((1, 1)));
        assert_eq!(mgr.query_id_by_finst(first), Some(query_id));
        assert_eq!(mgr.query_id_by_finst(second), None);
        mgr.inner
            .lock()
            .expect("query ctx manager lock")
            .active
            .get_mut(&execution)
            .expect("remaining fragment query")
            .total_fragments = Some(1);

        mgr.finish_fragment(query_id);
        mgr.unregister_finst(first);
        assert_eq!(mgr.fragment_counts_for_test(execution), None);
        assert_eq!(mgr.query_id_by_finst(first), None);
    }

    #[test]
    fn terminal_owner_retires_only_its_idle_native_attempt_without_waiting_for_expiry() {
        let mgr = Arc::new(test_manager());
        let query_id = QueryId::new(4_141, 4_142);
        let first = QueryExecutionKey::native_attempt(query_id, NonZeroU64::new(1).unwrap());
        let second = QueryExecutionKey::native_attempt(query_id, NonZeroU64::new(2).unwrap());
        mgr.ensure_native_context_execution(
            first,
            false,
            Duration::from_secs(1),
            Duration::from_secs(300),
        )
        .unwrap();
        assert_eq!(mgr.native_execution_resource_snapshot().active_contexts, 1);
        let weak = Arc::downgrade(&mgr);
        mgr.attach_cleanup_lease(
            first,
            QueryCleanupLease::from_release(move || {
                assert!(
                    weak.upgrade().unwrap().inner.try_lock().is_ok(),
                    "context cleanup must run outside the manager lock"
                );
            }),
        )
        .unwrap();

        assert!(!mgr.retire_idle_native_execution(second));
        assert_eq!(mgr.fragment_counts_for_test(first), Some((0, 0)));
        assert!(mgr.retire_idle_native_execution(first));
        assert_eq!(mgr.native_execution_resource_snapshot().active_contexts, 0);
        assert!(!mgr.retire_idle_native_execution(first));
        mgr.ensure_native_context_execution(
            second,
            false,
            Duration::from_secs(1),
            Duration::from_secs(300),
        )
        .expect("the successor attempt may own the released query slot");
    }

    #[test]
    fn terminal_owner_keeps_registered_native_fragments_until_they_stop() {
        let mgr = test_manager();
        let query_id = QueryId::new(4_151, 4_152);
        let execution = QueryExecutionKey::native_attempt(query_id, NonZeroU64::new(1).unwrap());
        let finst = UniqueId::new(4_153, 1);
        mgr.get_or_register_native_execution(
            execution,
            false,
            Duration::from_secs(1),
            Duration::from_secs(300),
        )
        .unwrap();
        mgr.register_native_finst_execution(finst, execution)
            .unwrap();

        assert!(!mgr.retire_idle_native_execution(execution));
        assert_eq!(mgr.fragment_counts_for_test(execution), Some((1, 1)));
        mgr.unregister_finst_execution(finst, execution);
        mgr.finish_fragment_execution(execution);
        assert_eq!(mgr.native_execution_resource_snapshot().active_contexts, 0);
    }

    #[test]
    fn concurrent_native_attempts_retire_only_their_own_context_generation() {
        let mgr = test_manager();
        let query_id = QueryId::new(4_161, 4_162);
        let first = QueryExecutionKey::native_attempt(query_id, NonZeroU64::new(1).unwrap());
        let second = QueryExecutionKey::native_attempt(query_id, NonZeroU64::new(2).unwrap());
        mgr.ensure_native_context_execution(
            first,
            false,
            Duration::from_secs(1),
            Duration::from_secs(300),
        )
        .unwrap();

        mgr.ensure_native_context_execution(
            second,
            false,
            Duration::from_secs(1),
            Duration::from_secs(300),
        )
        .expect("each native attempt owns an independent context");
        assert_eq!(mgr.native_execution_resource_snapshot().active_contexts, 2);
        assert_eq!(mgr.fragment_counts_for_test(first), Some((0, 0)));
        assert_eq!(mgr.fragment_counts_for_test(second), Some((0, 0)));
        assert!(mgr.query_mem_tracker_execution(first).is_some());
        assert!(mgr.query_mem_tracker_execution(second).is_some());

        assert!(mgr.retire_idle_native_execution(first));
        assert_eq!(mgr.native_execution_resource_snapshot().active_contexts, 1);
        assert!(mgr.query_mem_tracker_execution(first).is_none());
        assert!(mgr.query_mem_tracker_execution(second).is_some());
        assert!(!mgr.retire_idle_native_execution(first));
        assert!(mgr.retire_idle_native_execution(second));
        assert_eq!(mgr.native_execution_resource_snapshot().active_contexts, 0);
    }
}
