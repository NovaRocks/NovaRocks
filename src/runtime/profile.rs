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
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use crate::runtime::mem_tracker::MemTracker;
use crate::{metrics, runtime_profile};

#[derive(Clone, Debug)]
struct CounterSnapshot {
    name: String,
    unit: metrics::TUnit,
    strategy: runtime_profile::TCounterStrategy,
    value: i64,
    min_value: Option<i64>,
    max_value: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct RuntimeProfile {
    inner: Arc<RuntimeProfileInner>,
}

pub type Profiler = RuntimeProfile;

#[derive(Debug)]
struct RuntimeProfileInner {
    name: RwLock<String>,
    metadata: AtomicI64,
    counters: Mutex<HashMap<String, CounterRef>>,
    info_strings: Mutex<BTreeMap<String, String>>,
    children: Mutex<Vec<RuntimeProfile>>,
    child_map: Mutex<HashMap<String, RuntimeProfile>>,
}

impl RuntimeProfile {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(RuntimeProfileInner {
                name: RwLock::new(name.into()),
                metadata: AtomicI64::new(0),
                counters: Mutex::new(HashMap::new()),
                info_strings: Mutex::new(BTreeMap::new()),
                children: Mutex::new(Vec::new()),
                child_map: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub fn name(&self) -> String {
        self.inner
            .name
            .read()
            .map(|s| s.clone())
            .unwrap_or_else(|e| e.into_inner().clone())
    }

    pub fn set_name(&self, name: impl Into<String>) {
        let mut guard = self.inner.name.write().unwrap_or_else(|e| e.into_inner());
        *guard = name.into();
    }

    pub fn metadata(&self) -> i64 {
        self.inner.metadata.load(Ordering::Relaxed)
    }

    pub fn set_metadata(&self, md: i64) {
        self.inner.metadata.store(md, Ordering::Relaxed);
    }

    pub fn get_child(&self, name: &str) -> Option<RuntimeProfile> {
        self.inner
            .child_map
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(name)
            .cloned()
    }

    pub fn children(&self) -> Vec<RuntimeProfile> {
        self.inner
            .children
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn add_child(&self, child: RuntimeProfile) {
        let child_name = child.name();
        {
            let mut map = self
                .inner
                .child_map
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if map.contains_key(&child_name) {
                return;
            }
            map.insert(child_name.clone(), child.clone());
        }
        let mut children = self
            .inner
            .children
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        children.push(child);
    }

    pub fn child(&self, name: impl Into<String>) -> RuntimeProfile {
        let name = name.into();
        if let Some(existing) = self
            .inner
            .child_map
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&name)
            .cloned()
        {
            return existing;
        }
        let child = RuntimeProfile::new(name);
        self.add_child(child.clone());
        child
    }

    pub fn add_info_string(&self, key: impl Into<String>, value: impl Into<String>) {
        let mut guard = self
            .inner
            .info_strings
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        guard.insert(key.into(), value.into());
    }

    pub fn get_info_string(&self, key: &str) -> Option<String> {
        self.inner
            .info_strings
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
            .cloned()
    }

    pub fn copy_all_info_strings_from(&self, other: &RuntimeProfile) {
        let snapshot = other
            .inner
            .info_strings
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let mut guard = self
            .inner
            .info_strings
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for (k, v) in snapshot {
            guard.insert(k, v);
        }
    }

    pub fn copy_all_counters_from(&self, other: &RuntimeProfile) {
        let snapshots = other.counter_snapshots();
        for s in snapshots {
            let c = self.add_counter_with_strategy(s.name, s.unit, s.strategy);
            c.set(s.value);
            if let Some(min) = s.min_value {
                c.set_min(min);
            }
            if let Some(max) = s.max_value {
                c.set_max(max);
            }
        }
    }

    pub fn add_counter(&self, name: impl Into<String>, unit: metrics::TUnit) -> CounterRef {
        self.add_counter_with_strategy(name, unit, default_counter_strategy(unit))
    }

    pub fn add_counter_with_strategy(
        &self,
        name: impl Into<String>,
        unit: metrics::TUnit,
        strategy: runtime_profile::TCounterStrategy,
    ) -> CounterRef {
        let name = name.into();
        let mut guard = self
            .inner
            .counters
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(counter) = guard.get(&name) {
            return Arc::clone(counter);
        }
        let counter = Arc::new(Counter::new(name.clone(), unit, strategy));
        guard.insert(name, Arc::clone(&counter));
        counter
    }

    pub fn counter_add(&self, name: &str, unit: metrics::TUnit, delta: i64) {
        let c = self.add_counter(name.to_string(), unit);
        c.add(delta);
    }

    pub fn counter_set(&self, name: &str, unit: metrics::TUnit, value: i64) {
        let c = self.add_counter(name.to_string(), unit);
        c.set(value);
    }

    pub fn add_timer(&self, name: impl Into<String>) -> CounterRef {
        self.add_counter(name, metrics::TUnit::TIME_NS)
    }

    pub fn scoped_timer(&self, name: impl Into<String>) -> ScopedTimer {
        let counter = self.add_timer(name);
        ScopedTimer::new(counter)
    }

    pub fn to_thrift_tree(&self) -> runtime_profile::TRuntimeProfileTree {
        let mut nodes = Vec::new();
        self.to_thrift_nodes(&mut nodes);
        runtime_profile::TRuntimeProfileTree::new(nodes)
    }

    pub fn merge_isomorphic_profiles(profiles: &[RuntimeProfile]) -> RuntimeProfile {
        let first = profiles
            .first()
            .expect("merge_isomorphic_profiles requires non-empty input");

        let merged = RuntimeProfile::new(first.name());
        merged.set_metadata(first.metadata());
        merged.copy_all_info_strings_from(first);

        let all_counter_names: BTreeSet<String> = profiles
            .iter()
            .flat_map(|p| {
                p.inner
                    .counters
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .collect();

        for name in all_counter_names {
            let snapshots: Vec<CounterSnapshot> = profiles
                .iter()
                .filter_map(|p| p.counter_snapshot(&name))
                .collect();
            if snapshots.is_empty() {
                continue;
            }
            let unit = snapshots[0].unit;
            let strategy = snapshots[0].strategy.clone();
            let values: Vec<i64> = snapshots.iter().map(|s| s.value).collect();
            let (merged_value, min_value, max_value) = merge_counter_values(&strategy, &values);

            let c = merged.add_counter_with_strategy(name, unit, strategy);
            c.set(merged_value);
            c.set_min(min_value);
            c.set_max(max_value);
        }

        let children = first.children();
        for child in children {
            let child_name = child.name();
            let mut child_profiles = Vec::with_capacity(profiles.len());
            for p in profiles {
                if let Some(c) = p.get_child(&child_name) {
                    child_profiles.push(c);
                }
            }
            if child_profiles.len() != profiles.len() {
                continue;
            }
            let merged_child = RuntimeProfile::merge_isomorphic_profiles(&child_profiles);
            merged.add_child(merged_child);
        }

        merged
    }

    fn to_thrift_nodes(&self, out: &mut Vec<runtime_profile::TRuntimeProfileNode>) {
        let name = self.name();
        let metadata = self.metadata();

        let info_strings = self
            .inner
            .info_strings
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let info_strings_display_order = info_strings.keys().cloned().collect::<Vec<_>>();

        let counters = self
            .inner
            .counters
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .map(|c| c.to_thrift())
            .collect::<Vec<_>>();

        let children = self
            .inner
            .children
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();

        out.push(runtime_profile::TRuntimeProfileNode::new(
            name,
            children.len() as i32,
            counters,
            metadata,
            false,
            info_strings,
            info_strings_display_order,
            BTreeMap::<String, BTreeSet<String>>::new(),
            None,
        ));

        for child in children {
            child.to_thrift_nodes(out);
        }
    }

    fn counter_snapshot(&self, name: &str) -> Option<CounterSnapshot> {
        let guard = self
            .inner
            .counters
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let c = guard.get(name)?;
        let min_value = c
            .min_value
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let max_value = c
            .max_value
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        Some(CounterSnapshot {
            name: c.name.clone(),
            unit: c.unit,
            strategy: c.strategy.clone(),
            value: c.value(),
            min_value,
            max_value,
        })
    }

    fn counter_snapshots(&self) -> Vec<CounterSnapshot> {
        let guard = self
            .inner
            .counters
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        guard
            .values()
            .map(|c| {
                let min_value = c
                    .min_value
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone();
                let max_value = c
                    .max_value
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone();
                CounterSnapshot {
                    name: c.name.clone(),
                    unit: c.unit,
                    strategy: c.strategy.clone(),
                    value: c.value(),
                    min_value,
                    max_value,
                }
            })
            .collect()
    }
}

pub type CounterRef = Arc<Counter>;

#[derive(Debug)]
pub struct Counter {
    name: String,
    unit: metrics::TUnit,
    strategy: runtime_profile::TCounterStrategy,
    value: AtomicI64,
    min_value: Mutex<Option<i64>>,
    max_value: Mutex<Option<i64>>,
}

impl Counter {
    pub fn new(
        name: impl Into<String>,
        unit: metrics::TUnit,
        strategy: runtime_profile::TCounterStrategy,
    ) -> Self {
        Self {
            name: name.into(),
            unit,
            strategy,
            value: AtomicI64::new(0),
            min_value: Mutex::new(None),
            max_value: Mutex::new(None),
        }
    }

    pub fn add(&self, delta: i64) {
        self.value.fetch_add(delta, Ordering::Relaxed);
    }

    pub fn set(&self, value: i64) {
        self.value.store(value, Ordering::Relaxed);
    }

    pub fn value(&self) -> i64 {
        self.value.load(Ordering::Relaxed)
    }

    pub fn set_min(&self, min: i64) {
        let mut guard = self.min_value.lock().unwrap_or_else(|e| e.into_inner());
        *guard = Some(min);
    }

    pub fn set_max(&self, max: i64) {
        let mut guard = self.max_value.lock().unwrap_or_else(|e| e.into_inner());
        *guard = Some(max);
    }

    fn to_thrift(&self) -> runtime_profile::TCounter {
        let min_value = self
            .min_value
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let max_value = self
            .max_value
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        runtime_profile::TCounter::new(
            self.name.clone(),
            self.unit,
            self.value(),
            Some(self.strategy.clone()),
            min_value,
            max_value,
        )
    }
}

pub struct ScopedTimer {
    counter: CounterRef,
    start: Instant,
}

impl ScopedTimer {
    pub fn new(counter: CounterRef) -> Self {
        Self {
            counter,
            start: Instant::now(),
        }
    }
}

impl Drop for ScopedTimer {
    fn drop(&mut self) {
        let elapsed_ns = self.start.elapsed().as_nanos();
        let elapsed_ns = i64::try_from(elapsed_ns).unwrap_or(i64::MAX);
        self.counter.add(elapsed_ns);
    }
}

#[derive(Clone, Debug)]
pub struct OperatorProfiles {
    pub operator: RuntimeProfile,
    pub common: RuntimeProfile,
    pub unique: RuntimeProfile,
}

impl OperatorProfiles {
    pub fn new(operator: RuntimeProfile) -> Self {
        let common = operator.child("CommonMetrics");
        let unique = operator.child("UniqueMetrics");
        Self {
            operator,
            common,
            unique,
        }
    }
}

pub fn attach_mem_tracker_tree(profile: &RuntimeProfile, root: &Arc<MemTracker>) {
    let mem_root = profile.child("MemTracker");
    fill_mem_tracker_profile(&mem_root, root);
}

fn fill_mem_tracker_profile(profile: &RuntimeProfile, tracker: &Arc<MemTracker>) {
    profile.add_info_string("Label", tracker.label());
    let common = profile.child("CommonMetrics");
    common.counter_set(
        "CurrentMemoryBytes",
        metrics::TUnit::BYTES,
        tracker.current(),
    );
    common.counter_set("PeakMemoryBytes", metrics::TUnit::BYTES, tracker.peak());
    common.counter_set(
        "AllocatedMemoryBytes",
        metrics::TUnit::BYTES,
        tracker.allocated(),
    );
    common.counter_set(
        "DeallocatedMemoryBytes",
        metrics::TUnit::BYTES,
        tracker.deallocated(),
    );
    let _ = profile.child("UniqueMetrics");
    for child in tracker.children() {
        let child_profile = profile.child(child.label().to_string());
        fill_mem_tracker_profile(&child_profile, &child);
    }
}

pub fn default_counter_strategy(unit: metrics::TUnit) -> runtime_profile::TCounterStrategy {
    let aggregate_type = match unit {
        metrics::TUnit::CPU_TICKS
        | metrics::TUnit::TIME_NS
        | metrics::TUnit::TIME_MS
        | metrics::TUnit::TIME_S => runtime_profile::TCounterAggregateType::AVG,
        _ => runtime_profile::TCounterAggregateType::SUM,
    };
    runtime_profile::TCounterStrategy::new(
        aggregate_type,
        runtime_profile::TCounterMergeType::MERGE_ALL,
        0,
        runtime_profile::TCounterMinMaxType::MIN_MAX_ALL,
    )
}

pub fn clamp_u128_to_i64(value: u128) -> i64 {
    if value > i64::MAX as u128 {
        i64::MAX
    } else {
        value as i64
    }
}

fn merge_counter_values(
    strategy: &runtime_profile::TCounterStrategy,
    values: &[i64],
) -> (i64, i64, i64) {
    let min_value = values.iter().copied().min().unwrap_or(0);
    let max_value = values.iter().copied().max().unwrap_or(0);
    let n = i64::try_from(values.len()).unwrap_or(i64::MAX);
    let sum = values
        .iter()
        .copied()
        .fold(0i64, |acc, v| acc.saturating_add(v));
    let avg = if n <= 0 { 0 } else { sum / n };
    let value = match strategy.aggregate_type.0 {
        0 => sum, // SUM
        1 => avg, // AVG
        2 => sum, // SUM_AVG (sum at BE phase)
        3 => avg, // AVG_SUM (avg at BE phase)
        _ => sum,
    };
    (value, min_value, max_value)
}
