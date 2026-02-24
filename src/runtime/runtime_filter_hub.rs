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
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::common::ids::SlotId;
use crate::exec::node::RuntimeFilterProbeSpec;
use crate::exec::node::join::JoinRuntimeFilterSpec;
use crate::exec::pipeline::dependency::{DependencyHandle, DependencyManager};
use crate::exec::runtime_filter::{
    RuntimeInFilter, RuntimeMembershipFilter, StarrocksRuntimeFilterType,
    decode_starrocks_in_filter, decode_starrocks_membership_filter, peek_starrocks_filter_type,
};
use crate::novarocks_logging::{debug, warn};

#[derive(Clone)]
pub(crate) struct RuntimeFilterHandle {
    inner: Arc<RuntimeFilterHandleInner>,
}

#[derive(Default)]
struct RuntimeFilterHandleInner {
    store: RwLock<RuntimeFilterStore>,
    version: AtomicU64,
}

#[derive(Default)]
struct RuntimeFilterStore {
    in_filters: HashMap<i32, Arc<RuntimeInFilter>>,
    membership_filters: HashMap<i32, Arc<RuntimeMembershipFilter>>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RuntimeFilterSnapshot {
    in_filters: Vec<Arc<RuntimeInFilter>>,
    membership_filters: Vec<Arc<RuntimeMembershipFilter>>,
}

impl RuntimeFilterHandle {
    fn new() -> Self {
        Self {
            inner: Arc::new(RuntimeFilterHandleInner {
                store: RwLock::new(RuntimeFilterStore::default()),
                version: AtomicU64::new(0),
            }),
        }
    }

    pub(crate) fn snapshot(&self) -> RuntimeFilterSnapshot {
        let guard = self.inner.store.read().expect("runtime filter handle lock");
        let mut in_filters = Vec::with_capacity(guard.in_filters.len());
        let mut membership_filters = Vec::with_capacity(guard.membership_filters.len());
        for filter in guard.in_filters.values() {
            in_filters.push(Arc::clone(filter));
        }
        for filter in guard.membership_filters.values() {
            membership_filters.push(Arc::clone(filter));
        }
        RuntimeFilterSnapshot {
            in_filters,
            membership_filters,
        }
    }

    fn update_in_filter(&self, filter: RuntimeInFilter) {
        let mut guard = self
            .inner
            .store
            .write()
            .expect("runtime filter handle lock");
        guard
            .in_filters
            .insert(filter.filter_id(), Arc::new(filter));
        self.inner.version.fetch_add(1, Ordering::AcqRel);
    }

    fn update_membership_filter(&self, filter: RuntimeMembershipFilter) {
        let mut guard = self
            .inner
            .store
            .write()
            .expect("runtime filter handle lock");
        guard
            .membership_filters
            .insert(filter.filter_id(), Arc::new(filter));
        self.inner.version.fetch_add(1, Ordering::AcqRel);
    }

    fn counts(&self) -> (usize, usize) {
        let guard = self.inner.store.read().expect("runtime filter handle lock");
        (guard.in_filters.len(), guard.membership_filters.len())
    }

    pub(crate) fn version(&self) -> u64 {
        self.inner.version.load(Ordering::Acquire)
    }
}

impl RuntimeFilterSnapshot {
    pub(crate) fn from_filters(
        in_filters: Vec<RuntimeInFilter>,
        membership_filters: Vec<RuntimeMembershipFilter>,
    ) -> Self {
        let in_filters = in_filters.into_iter().map(Arc::new).collect();
        let membership_filters = membership_filters.into_iter().map(Arc::new).collect();
        Self {
            in_filters,
            membership_filters,
        }
    }

    pub(crate) fn in_filters(&self) -> &[Arc<RuntimeInFilter>] {
        &self.in_filters
    }

    pub(crate) fn membership_filters(&self) -> &[Arc<RuntimeMembershipFilter>] {
        &self.membership_filters
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.in_filters.is_empty() && self.membership_filters.is_empty()
    }
}

pub(crate) struct RuntimeFilterHub {
    dep_manager: DependencyManager,
    entries: Mutex<HashMap<i32, Arc<RuntimeFilterEntry>>>,
    build_specs: Mutex<HashMap<i32, RuntimeFilterBuildSpec>>,
    probe_targets: Mutex<HashMap<i32, Vec<RuntimeFilterTarget>>>,
    probe_specs_by_node: Mutex<HashMap<i32, HashMap<i32, SlotId>>>,
    expected_by_node: Mutex<HashMap<i32, usize>>,
    local_deps: Mutex<HashMap<i32, DependencyHandle>>,
    published_in_filters: Mutex<HashMap<i32, RuntimeInFilter>>,
    published_membership_filters: Mutex<HashMap<i32, RuntimeMembershipFilter>>,
    scan_wait_timeout_ms: AtomicI64,
    wait_timeout_ms: AtomicI64,
}

#[derive(Clone)]
pub(crate) struct RuntimeFilterProbe {
    inner: Arc<RuntimeFilterProbeInner>,
}

struct RuntimeFilterEntry {
    dep: DependencyHandle,
    handle: RuntimeFilterHandle,
    membership_seen: Mutex<HashSet<i32>>,
}

struct RuntimeFilterProbeInner {
    entry: Arc<RuntimeFilterEntry>,
    scan_wait_timeout: Option<Duration>,
    wait_timeout: Option<Duration>,
    wait_start: Mutex<Option<Instant>>,
    timeout_armed: AtomicBool,
}

enum RuntimeFilterUpdate {
    In(RuntimeInFilter),
    Membership(RuntimeMembershipFilter),
}

#[derive(Clone, Debug)]
struct RuntimeFilterBuildSpec {
    slot_id: SlotId,
}

#[derive(Clone, Debug)]
struct RuntimeFilterTarget {
    node_id: i32,
    slot_id: SlotId,
}

impl RuntimeFilterHub {
    pub(crate) fn new(dep_manager: DependencyManager) -> Self {
        Self {
            dep_manager,
            entries: Mutex::new(HashMap::new()),
            build_specs: Mutex::new(HashMap::new()),
            probe_targets: Mutex::new(HashMap::new()),
            probe_specs_by_node: Mutex::new(HashMap::new()),
            expected_by_node: Mutex::new(HashMap::new()),
            local_deps: Mutex::new(HashMap::new()),
            published_in_filters: Mutex::new(HashMap::new()),
            published_membership_filters: Mutex::new(HashMap::new()),
            scan_wait_timeout_ms: AtomicI64::new(-1),
            wait_timeout_ms: AtomicI64::new(-1),
        }
    }

    pub(crate) fn set_wait_timeouts(
        &self,
        scan_timeout: Option<Duration>,
        wait_timeout: Option<Duration>,
    ) {
        let scan_ms = duration_to_ms(scan_timeout);
        let wait_ms = duration_to_ms(wait_timeout);
        self.scan_wait_timeout_ms.store(scan_ms, Ordering::Release);
        self.wait_timeout_ms.store(wait_ms, Ordering::Release);
    }

    pub(crate) fn register_filter_specs(&self, node_id: i32, specs: &[JoinRuntimeFilterSpec]) {
        let _ = node_id;
        if specs.is_empty() {
            return;
        }
        let mut guard = self.build_specs.lock().expect("runtime filter hub lock");
        for spec in specs {
            guard.insert(
                spec.filter_id,
                RuntimeFilterBuildSpec {
                    slot_id: spec.probe_slot_id,
                },
            );
        }
        debug!(
            "runtime filter build specs registered: node_id={} count={} filter_ids={:?}",
            node_id,
            specs.len(),
            specs.iter().map(|spec| spec.filter_id).collect::<Vec<_>>()
        );
    }

    pub(crate) fn register_probe_specs(&self, node_id: i32, specs: &[RuntimeFilterProbeSpec]) {
        if specs.is_empty() {
            return;
        }
        let (expected, snapshot) = {
            let mut specs_guard = self
                .probe_specs_by_node
                .lock()
                .expect("runtime filter hub lock");
            let entry = specs_guard.entry(node_id).or_insert_with(HashMap::new);
            for spec in specs {
                if let Some(existing) = entry.get(&spec.filter_id) {
                    if *existing != spec.slot_id {
                        warn!(
                            "runtime filter probe spec mismatch: node_id={} filter_id={} existing_slot_id={:?} new_slot_id={:?}",
                            node_id, spec.filter_id, existing, spec.slot_id
                        );
                    }
                    continue;
                }
                entry.insert(spec.filter_id, spec.slot_id);
            }
            (entry.len(), entry.clone())
        };
        {
            let mut expected_guard = self
                .expected_by_node
                .lock()
                .expect("runtime filter hub lock");
            expected_guard.insert(node_id, expected);
        }
        {
            let mut targets_guard = self.probe_targets.lock().expect("runtime filter hub lock");
            for (&filter_id, &slot_id) in snapshot.iter() {
                let targets = targets_guard.entry(filter_id).or_insert_with(Vec::new);
                if targets.iter().any(|t| t.node_id == node_id) {
                    continue;
                }
                targets.push(RuntimeFilterTarget { node_id, slot_id });
            }
        }
        debug!(
            "runtime filter probe specs registered: node_id={} expected={} filters={:?}",
            node_id,
            expected,
            snapshot
                .iter()
                .map(|(filter_id, slot_id)| (*filter_id, *slot_id))
                .collect::<Vec<_>>()
        );
        let entry = self.get_or_create_entry(node_id);
        self.maybe_mark_ready(node_id, &entry);
        self.replay_cached_filters(node_id, entry);
    }

    pub(crate) fn filter_spec_slot_id(&self, filter_id: i32) -> Option<SlotId> {
        {
            let guard = self.build_specs.lock().expect("runtime filter hub lock");
            if let Some(spec) = guard.get(&filter_id) {
                return Some(spec.slot_id);
            }
        }
        let guard = self.probe_targets.lock().expect("runtime filter hub lock");
        guard
            .get(&filter_id)
            .and_then(|targets| targets.first().map(|t| t.slot_id))
    }

    pub(crate) fn local_dependency(&self, build_node_id: i32) -> DependencyHandle {
        let mut guard = self.local_deps.lock().expect("runtime filter hub lock");
        if let Some(dep) = guard.get(&build_node_id) {
            return dep.clone();
        }
        let dep = self
            .dep_manager
            .get_or_create(format!("local_rf:{}", build_node_id));
        debug!(
            "local runtime filter dependency created: build_node_id={} dep_id={} name={}",
            build_node_id,
            dep.id(),
            dep.name()
        );
        guard.insert(build_node_id, dep.clone());
        dep
    }

    pub(crate) fn local_dependency_if_exists(
        &self,
        build_node_id: i32,
    ) -> Option<DependencyHandle> {
        let guard = self.local_deps.lock().expect("runtime filter hub lock");
        guard.get(&build_node_id).cloned()
    }

    pub(crate) fn mark_local_filters_ready(&self, build_node_id: i32) {
        let dep = self.local_dependency(build_node_id);
        let was_ready = dep.is_ready();
        dep.set_ready();
        debug!(
            "local runtime filter dependency marked ready: build_node_id={} dep_id={} name={} was_ready={}",
            build_node_id,
            dep.id(),
            dep.name(),
            was_ready
        );
    }

    pub(crate) fn register_probe(&self, node_id: i32) -> RuntimeFilterProbe {
        let entry = self.get_or_create_entry(node_id);
        let scan_wait_timeout = self.scan_wait_timeout();
        let wait_timeout = self.wait_timeout();
        RuntimeFilterProbe {
            inner: Arc::new(RuntimeFilterProbeInner {
                entry,
                scan_wait_timeout,
                wait_timeout,
                wait_start: Mutex::new(None),
                timeout_armed: AtomicBool::new(false),
            }),
        }
    }

    pub(crate) fn publish_filters(
        &self,
        in_filters: &[RuntimeInFilter],
        membership_filters: &[RuntimeMembershipFilter],
    ) {
        if !in_filters.is_empty() || !membership_filters.is_empty() {
            debug!(
                "runtime filter publish local: in_filters={} membership_filters={} in_filter_ids={:?} membership_filter_ids={:?}",
                in_filters.len(),
                membership_filters.len(),
                in_filters
                    .iter()
                    .map(|filter| filter.filter_id())
                    .collect::<Vec<_>>(),
                membership_filters
                    .iter()
                    .map(|filter| filter.filter_id())
                    .collect::<Vec<_>>()
            );
        }
        for filter in in_filters {
            self.publish_in_filter_to_targets(filter.filter_id(), filter);
        }
        for filter in membership_filters {
            self.publish_membership_filter_to_targets(filter.filter_id(), filter);
        }
    }

    pub(crate) fn receive_remote_filter(&self, filter_id: i32, data: &[u8]) -> Result<(), String> {
        let targets = {
            let guard = self.probe_targets.lock().expect("runtime filter hub lock");
            guard
                .get(&filter_id)
                .cloned()
                .ok_or_else(|| format!("runtime filter targets not found: filter_id={filter_id}"))?
        };
        let Some(first) = targets.first() else {
            return Err(format!(
                "runtime filter targets empty: filter_id={filter_id}"
            ));
        };
        let rf_type = peek_starrocks_filter_type(data)?;
        debug!(
            "runtime filter receive remote: filter_id={} type={:?} bytes={}",
            filter_id,
            rf_type,
            data.len()
        );
        if rf_type == StarrocksRuntimeFilterType::In {
            let filter = decode_starrocks_in_filter(filter_id, first.slot_id, data)?;
            {
                let mut guard = self
                    .published_in_filters
                    .lock()
                    .expect("runtime filter cache lock");
                guard.insert(filter_id, filter.clone());
            }
            for target in targets {
                let filter_for_node = if target.slot_id == filter.slot_id() {
                    filter.clone()
                } else {
                    filter.with_slot_id(target.slot_id)
                };
                self.add_filter_to_node(
                    target.node_id,
                    filter_id,
                    RuntimeFilterUpdate::In(filter_for_node),
                );
            }
        } else {
            let filter = decode_starrocks_membership_filter(filter_id, first.slot_id, data)?;
            {
                let mut guard = self
                    .published_membership_filters
                    .lock()
                    .expect("runtime filter cache lock");
                guard.insert(filter_id, filter.clone());
            }
            for target in targets {
                let filter_for_node = if target.slot_id == filter.slot_id() {
                    filter.clone()
                } else {
                    filter.with_slot_id(target.slot_id)
                };
                self.add_filter_to_node(
                    target.node_id,
                    filter_id,
                    RuntimeFilterUpdate::Membership(filter_for_node),
                );
            }
        }
        Ok(())
    }

    fn add_filter_to_node(&self, node_id: i32, filter_id: i32, update: RuntimeFilterUpdate) {
        let entry = self.get_or_create_entry(node_id);
        match update {
            RuntimeFilterUpdate::In(filter) => {
                entry.handle.update_in_filter(filter);
            }
            RuntimeFilterUpdate::Membership(filter) => {
                entry.handle.update_membership_filter(filter);
                {
                    let mut guard = entry.membership_seen.lock().expect("runtime filter lock");
                    guard.insert(filter_id);
                }
                self.maybe_mark_ready(node_id, &entry);
            }
        }
    }

    fn publish_in_filter_to_targets(&self, filter_id: i32, filter: &RuntimeInFilter) {
        {
            let mut guard = self
                .published_in_filters
                .lock()
                .expect("runtime filter cache lock");
            guard.insert(filter_id, filter.clone());
        }
        let targets = {
            let guard = self.probe_targets.lock().expect("runtime filter hub lock");
            guard.get(&filter_id).cloned().unwrap_or_default()
        };
        if targets.is_empty() {
            debug!(
                "runtime filter publish deferred: filter_id={} reason=no_targets",
                filter_id
            );
            return;
        }
        debug!(
            "runtime filter publish in filter: filter_id={} targets={}",
            filter_id,
            targets.len()
        );
        for target in targets {
            let filter_for_node = if target.slot_id == filter.slot_id() {
                filter.clone()
            } else {
                filter.with_slot_id(target.slot_id)
            };
            self.add_filter_to_node(
                target.node_id,
                filter_id,
                RuntimeFilterUpdate::In(filter_for_node),
            );
        }
    }

    fn publish_membership_filter_to_targets(
        &self,
        filter_id: i32,
        filter: &RuntimeMembershipFilter,
    ) {
        {
            let mut guard = self
                .published_membership_filters
                .lock()
                .expect("runtime filter cache lock");
            guard.insert(filter_id, filter.clone());
        }
        let targets = {
            let guard = self.probe_targets.lock().expect("runtime filter hub lock");
            guard.get(&filter_id).cloned().unwrap_or_default()
        };
        if targets.is_empty() {
            debug!(
                "runtime filter publish deferred: filter_id={} reason=no_targets",
                filter_id
            );
            return;
        }
        debug!(
            "runtime filter publish membership filter: filter_id={} targets={}",
            filter_id,
            targets.len()
        );
        for target in targets {
            let filter_for_node = if target.slot_id == filter.slot_id() {
                filter.clone()
            } else {
                filter.with_slot_id(target.slot_id)
            };
            self.add_filter_to_node(
                target.node_id,
                filter_id,
                RuntimeFilterUpdate::Membership(filter_for_node),
            );
        }
    }

    fn get_or_create_entry(&self, node_id: i32) -> Arc<RuntimeFilterEntry> {
        let mut guard = self.entries.lock().expect("runtime filter hub lock");
        if let Some(entry) = guard.get(&node_id) {
            return Arc::clone(entry);
        }
        let dep = self
            .dep_manager
            .get_or_create(format!("runtime_filter:{}", node_id));
        let entry = Arc::new(RuntimeFilterEntry {
            dep,
            handle: RuntimeFilterHandle::new(),
            membership_seen: Mutex::new(HashSet::new()),
        });
        guard.insert(node_id, Arc::clone(&entry));
        entry
    }

    fn scan_wait_timeout(&self) -> Option<Duration> {
        ms_to_duration(self.scan_wait_timeout_ms.load(Ordering::Acquire))
    }

    fn wait_timeout(&self) -> Option<Duration> {
        ms_to_duration(self.wait_timeout_ms.load(Ordering::Acquire))
    }

    fn maybe_mark_ready(&self, node_id: i32, entry: &RuntimeFilterEntry) {
        let expected = {
            let guard = self
                .expected_by_node
                .lock()
                .expect("runtime filter hub lock");
            guard.get(&node_id).copied()
        };
        let Some(expected) = expected else {
            return;
        };
        if expected == 0 {
            entry.dep.set_ready();
            return;
        }
        let received = {
            let guard = entry.membership_seen.lock().expect("runtime filter lock");
            guard.len()
        };
        if received < expected {
            return;
        }
        if entry.dep.is_ready() {
            return;
        }
        let (in_filters, membership_filters) = entry.handle.counts();
        debug!(
            "runtime filter ready for node: node_id={} expected={} received={} in_filters={} membership_filters={}",
            node_id, expected, received, in_filters, membership_filters
        );
        entry.dep.set_ready();
    }

    fn replay_cached_filters(&self, node_id: i32, entry: Arc<RuntimeFilterEntry>) {
        let specs = {
            let guard = self
                .probe_specs_by_node
                .lock()
                .expect("runtime filter hub lock");
            guard.get(&node_id).cloned()
        };
        let Some(specs) = specs else {
            return;
        };

        // Replay cached filters for late probe registration.
        let cached_in = {
            let guard = self
                .published_in_filters
                .lock()
                .expect("runtime filter cache lock");
            specs
                .iter()
                .filter_map(|(&filter_id, &slot_id)| {
                    guard
                        .get(&filter_id)
                        .cloned()
                        .map(|filter| (filter_id, slot_id, filter))
                })
                .collect::<Vec<_>>()
        };
        for (filter_id, slot_id, filter) in cached_in {
            let filter_for_node = if slot_id == filter.slot_id() {
                filter
            } else {
                filter.with_slot_id(slot_id)
            };
            self.add_filter_to_node(node_id, filter_id, RuntimeFilterUpdate::In(filter_for_node));
        }

        let cached_membership = {
            let guard = self
                .published_membership_filters
                .lock()
                .expect("runtime filter cache lock");
            specs
                .iter()
                .filter_map(|(&filter_id, &slot_id)| {
                    guard
                        .get(&filter_id)
                        .cloned()
                        .map(|filter| (filter_id, slot_id, filter))
                })
                .collect::<Vec<_>>()
        };
        for (filter_id, slot_id, filter) in cached_membership {
            let filter_for_node = if slot_id == filter.slot_id() {
                filter
            } else {
                filter.with_slot_id(slot_id)
            };
            self.add_filter_to_node(
                node_id,
                filter_id,
                RuntimeFilterUpdate::Membership(filter_for_node),
            );
        }

        self.maybe_mark_ready(node_id, &entry);
    }
}

impl RuntimeFilterProbe {
    pub(crate) fn dependency_or_timeout(&self, on_scan_node: bool) -> Option<DependencyHandle> {
        let dep = self.inner.entry.dep.clone();
        if dep.is_ready() {
            return None;
        }
        let timeout = if on_scan_node {
            self.inner.scan_wait_timeout
        } else {
            self.inner.wait_timeout
        };
        let Some(timeout) = timeout else {
            // No wait timeout configured: do not block on runtime filters.
            dep.set_ready();
            return None;
        };
        if timeout.is_zero() {
            dep.set_ready();
            return None;
        }
        let mut guard = self
            .inner
            .wait_start
            .lock()
            .expect("runtime filter wait lock");
        let start = guard.get_or_insert_with(Instant::now);
        let elapsed = start.elapsed();
        if elapsed >= timeout {
            dep.set_ready();
            return None;
        }
        if !self.inner.timeout_armed.swap(true, Ordering::AcqRel) {
            let dep_clone = dep.clone();
            let sleep_for = timeout.saturating_sub(elapsed);
            std::thread::spawn(move || {
                std::thread::sleep(sleep_for);
                dep_clone.set_ready();
            });
        }
        Some(dep)
    }

    pub(crate) fn handle(&self) -> RuntimeFilterHandle {
        self.inner.entry.handle.clone()
    }

    pub(crate) fn dependency(&self) -> DependencyHandle {
        self.inner.entry.dep.clone()
    }

    pub(crate) fn snapshot(&self) -> RuntimeFilterSnapshot {
        self.inner.entry.handle.snapshot()
    }

    #[allow(dead_code)]
    pub(crate) fn in_filters(&self) -> Vec<Arc<RuntimeInFilter>> {
        self.inner.entry.handle.snapshot().in_filters
    }

    #[allow(dead_code)]
    pub(crate) fn membership_filters(&self) -> Vec<Arc<RuntimeMembershipFilter>> {
        self.inner.entry.handle.snapshot().membership_filters
    }

    pub(crate) fn mark_ready(&self) -> Option<Duration> {
        if !self.inner.entry.dep.is_ready() {
            return None;
        }
        let mut guard = self
            .inner
            .wait_start
            .lock()
            .expect("runtime filter wait lock");
        let start = guard.take()?;
        Some(start.elapsed())
    }
}

fn duration_to_ms(timeout: Option<Duration>) -> i64 {
    timeout.map(|v| v.as_millis() as i64).unwrap_or(-1)
}

fn ms_to_duration(ms: i64) -> Option<Duration> {
    if ms < 0 {
        None
    } else {
        Some(Duration::from_millis(ms as u64))
    }
}
