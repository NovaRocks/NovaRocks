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

use crate::proto::novarocks;
use crate::runtime::mem_tracker::MemTracker;
use crate::thrift::{metrics, runtime_profile};

#[derive(Clone, Debug)]
struct CounterSnapshot {
    name: String,
    parent_name: String,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ProfileUnit {
    Unit,
    CpuTicks,
    Bytes,
    TimeNs,
    TimeMs,
    TimeS,
    None,
}

impl ProfileUnit {
    pub(crate) fn to_proto(self) -> novarocks::ProfileUnit {
        match self {
            Self::Unit => novarocks::ProfileUnit::Unit,
            Self::CpuTicks => novarocks::ProfileUnit::CpuTicks,
            Self::Bytes => novarocks::ProfileUnit::Bytes,
            Self::TimeNs => novarocks::ProfileUnit::TimeNs,
            Self::TimeMs => novarocks::ProfileUnit::TimeMs,
            Self::TimeS => novarocks::ProfileUnit::TimeS,
            Self::None => novarocks::ProfileUnit::None,
        }
    }

    pub(crate) fn from_proto(unit: i32) -> Result<Self, String> {
        match novarocks::ProfileUnit::try_from(unit) {
            Ok(novarocks::ProfileUnit::Unit) => Ok(Self::Unit),
            Ok(novarocks::ProfileUnit::CpuTicks) => Ok(Self::CpuTicks),
            Ok(novarocks::ProfileUnit::Bytes) => Ok(Self::Bytes),
            Ok(novarocks::ProfileUnit::TimeNs) => Ok(Self::TimeNs),
            Ok(novarocks::ProfileUnit::TimeMs) => Ok(Self::TimeMs),
            Ok(novarocks::ProfileUnit::TimeS) => Ok(Self::TimeS),
            Ok(novarocks::ProfileUnit::None) => Ok(Self::None),
            Ok(novarocks::ProfileUnit::Unspecified) => {
                Err("ProfileUnit is unspecified in native runtime profile".to_string())
            }
            Err(_) => Err(format!(
                "unknown ProfileUnit value {unit} in native runtime profile"
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CounterAggregateType {
    Sum,
    Avg,
    SumAvg,
    AvgSum,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CounterStrategy {
    aggregate_type: CounterAggregateType,
}

impl CounterStrategy {
    pub fn new(aggregate_type: CounterAggregateType) -> Self {
        Self { aggregate_type }
    }

    pub fn aggregate_type(self) -> CounterAggregateType {
        self.aggregate_type
    }
}

pub fn default_native_counter_strategy(unit: ProfileUnit) -> CounterStrategy {
    let aggregate_type = match unit {
        ProfileUnit::CpuTicks | ProfileUnit::TimeNs | ProfileUnit::TimeMs | ProfileUnit::TimeS => {
            CounterAggregateType::Avg
        }
        ProfileUnit::Unit | ProfileUnit::Bytes | ProfileUnit::None => CounterAggregateType::Sum,
    };
    CounterStrategy::new(aggregate_type)
}

#[derive(Debug)]
struct RuntimeProfileInner {
    name: RwLock<String>,
    metadata: AtomicI64,
    counters: Mutex<HashMap<String, CounterEntry>>,
    info_strings: Mutex<BTreeMap<String, String>>,
    children: Mutex<Vec<RuntimeProfile>>,
    child_map: Mutex<HashMap<String, RuntimeProfile>>,
}

const ROOT_COUNTER: &str = "";

#[derive(Clone, Debug)]
struct CounterEntry {
    counter: CounterRef,
    parent_name: String,
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
            let c = self.add_counter_with_parent_and_strategy(
                s.name,
                s.unit,
                s.strategy,
                s.parent_name,
            );
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
        self.add_counter_with_parent_and_strategy(
            name,
            unit,
            default_counter_strategy(unit),
            ROOT_COUNTER,
        )
    }

    pub fn add_unit_counter(&self, name: impl Into<String>) -> CounterRef {
        self.add_counter(name, metrics::TUnit::UNIT)
    }

    pub fn add_bytes_counter(&self, name: impl Into<String>) -> CounterRef {
        self.add_counter(name, metrics::TUnit::BYTES)
    }

    pub fn add_child_counter(
        &self,
        name: impl Into<String>,
        unit: metrics::TUnit,
        parent_name: impl Into<String>,
    ) -> CounterRef {
        self.add_counter_with_parent_and_strategy(
            name,
            unit,
            default_counter_strategy(unit),
            parent_name,
        )
    }

    pub fn add_counter_with_strategy(
        &self,
        name: impl Into<String>,
        unit: metrics::TUnit,
        strategy: runtime_profile::TCounterStrategy,
    ) -> CounterRef {
        self.add_counter_with_parent_and_strategy(name, unit, strategy, ROOT_COUNTER)
    }

    pub fn add_counter_with_parent_and_strategy(
        &self,
        name: impl Into<String>,
        unit: metrics::TUnit,
        strategy: runtime_profile::TCounterStrategy,
        parent_name: impl Into<String>,
    ) -> CounterRef {
        let name = name.into();
        let parent_name = parent_name.into();
        let mut guard = self
            .inner
            .counters
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = guard.get(&name) {
            return Arc::clone(&entry.counter);
        }
        let counter = Arc::new(Counter::new(name.clone(), unit, strategy));
        guard.insert(
            name,
            CounterEntry {
                counter: Arc::clone(&counter),
                parent_name,
            },
        );
        counter
    }

    pub fn counter_add(&self, name: &str, unit: metrics::TUnit, delta: i64) {
        let c = self.add_counter(name.to_string(), unit);
        c.add(delta);
    }

    pub fn counter_add_unit(&self, name: &str, delta: i64) {
        self.counter_add(name, metrics::TUnit::UNIT, delta);
    }

    pub fn counter_add_bytes(&self, name: &str, delta: i64) {
        self.counter_add(name, metrics::TUnit::BYTES, delta);
    }

    pub fn counter_add_with_parent(
        &self,
        name: &str,
        unit: metrics::TUnit,
        delta: i64,
        parent_name: &str,
    ) {
        let c = self.add_child_counter(name.to_string(), unit, parent_name.to_string());
        c.add(delta);
    }

    pub fn counter_set(&self, name: &str, unit: metrics::TUnit, value: i64) {
        let c = self.add_counter(name.to_string(), unit);
        c.set(value);
    }

    pub fn counter_set_unit(&self, name: &str, value: i64) {
        self.counter_set(name, metrics::TUnit::UNIT, value);
    }

    pub fn counter_set_bytes(&self, name: &str, value: i64) {
        self.counter_set(name, metrics::TUnit::BYTES, value);
    }

    pub(crate) fn counter_value(&self, name: &str) -> Option<i64> {
        self.counter_snapshot(name).map(|snapshot| snapshot.value)
    }

    pub(crate) fn counter_value_min_max(&self, name: &str) -> Option<(i64, i64, i64)> {
        self.counter_snapshot(name).map(|snapshot| {
            let value = snapshot.value;
            (
                value,
                snapshot.min_value.unwrap_or(value),
                snapshot.max_value.unwrap_or(value),
            )
        })
    }

    pub fn add_timer(&self, name: impl Into<String>) -> CounterRef {
        self.add_counter(name, metrics::TUnit::TIME_NS)
    }

    pub fn add_child_timer(
        &self,
        name: impl Into<String>,
        parent_name: impl Into<String>,
    ) -> CounterRef {
        self.add_child_counter(name, metrics::TUnit::TIME_NS, parent_name)
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

    pub(crate) fn to_proto(&self) -> novarocks::RuntimeProfileTree {
        novarocks::RuntimeProfileTree {
            root: Some(self.to_proto_node()),
        }
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
            let parent_name = snapshots[0].parent_name.clone();
            let values: Vec<i64> = snapshots.iter().map(|s| s.value).collect();
            let (merged_value, min_value, max_value) = merge_counter_values(&strategy, &values);

            let c = merged.add_counter_with_parent_and_strategy(name, unit, strategy, parent_name);
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
            .map(|entry| entry.counter.to_thrift())
            .collect::<Vec<_>>();

        let child_counters_map = self
            .inner
            .counters
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .fold(
                BTreeMap::<String, BTreeSet<String>>::new(),
                |mut acc, (name, entry)| {
                    acc.entry(entry.parent_name.clone())
                        .or_default()
                        .insert(name.clone());
                    acc
                },
            );

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
            child_counters_map,
            None,
        ));

        for child in children {
            child.to_thrift_nodes(out);
        }
    }

    fn to_proto_node(&self) -> novarocks::ProfileNode {
        let info_strings = self
            .inner
            .info_strings
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let mut counter_snapshots = self.counter_snapshots();
        counter_snapshots.sort_by(|left, right| {
            left.parent_name
                .cmp(&right.parent_name)
                .then_with(|| left.name.cmp(&right.name))
        });
        let counters = counter_snapshots
            .into_iter()
            .map(counter_snapshot_to_proto)
            .collect();
        let children = self
            .children()
            .into_iter()
            .map(|child| child.to_proto_node())
            .collect();

        novarocks::ProfileNode {
            name: self.name(),
            node_id: metadata_to_proto_node_id(self.metadata()),
            counters,
            info_strings: info_strings.into_iter().collect(),
            children,
        }
    }

    fn counter_snapshot(&self, name: &str) -> Option<CounterSnapshot> {
        let guard = self
            .inner
            .counters
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let entry = guard.get(name)?;
        let c = &entry.counter;
        let min_value = *c.min_value.lock().unwrap_or_else(|e| e.into_inner());
        let max_value = *c.max_value.lock().unwrap_or_else(|e| e.into_inner());
        Some(CounterSnapshot {
            name: c.name.clone(),
            parent_name: entry.parent_name.clone(),
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
            .map(|entry| {
                let c = &entry.counter;
                let min_value = *c.min_value.lock().unwrap_or_else(|e| e.into_inner());
                let max_value = *c.max_value.lock().unwrap_or_else(|e| e.into_inner());
                CounterSnapshot {
                    name: c.name.clone(),
                    parent_name: entry.parent_name.clone(),
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

fn metadata_to_proto_node_id(metadata: i64) -> i32 {
    match i32::try_from(metadata) {
        Ok(value) => value,
        Err(_) if metadata.is_negative() => i32::MIN,
        Err(_) => i32::MAX,
    }
}

fn counter_snapshot_to_proto(snapshot: CounterSnapshot) -> novarocks::Counter {
    novarocks::Counter {
        name: snapshot.name,
        parent_name: snapshot.parent_name,
        unit: unit_to_proto(snapshot.unit) as i32,
        value: snapshot.value,
        min_value: snapshot.min_value,
        max_value: snapshot.max_value,
    }
}

fn unit_to_proto(unit: metrics::TUnit) -> novarocks::ProfileUnit {
    use novarocks::ProfileUnit as P;

    match unit {
        metrics::TUnit::UNIT => P::Unit,
        metrics::TUnit::CPU_TICKS => P::CpuTicks,
        metrics::TUnit::BYTES => P::Bytes,
        metrics::TUnit::TIME_NS => P::TimeNs,
        metrics::TUnit::TIME_MS => P::TimeMs,
        metrics::TUnit::TIME_S => P::TimeS,
        _ => P::None,
    }
}

pub(crate) fn native_profile_tree_to_thrift(
    tree: &novarocks::RuntimeProfileTree,
) -> Result<runtime_profile::TRuntimeProfileTree, String> {
    let root = tree
        .root
        .as_ref()
        .ok_or_else(|| "RuntimeProfileTree missing root".to_string())?;
    let mut nodes = Vec::new();
    native_profile_node_to_thrift(root, &mut nodes)?;
    Ok(runtime_profile::TRuntimeProfileTree::new(nodes))
}

fn native_profile_node_to_thrift(
    node: &novarocks::ProfileNode,
    out: &mut Vec<runtime_profile::TRuntimeProfileNode>,
) -> Result<(), String> {
    let mut counters = node
        .counters
        .iter()
        .map(native_counter_to_thrift)
        .collect::<Result<Vec<_>, _>>()?;
    counters.sort_by(|left, right| left.name.cmp(&right.name));

    let mut child_counters_map = BTreeMap::<String, BTreeSet<String>>::new();
    for counter in &node.counters {
        child_counters_map
            .entry(counter.parent_name.clone())
            .or_default()
            .insert(counter.name.clone());
    }

    let info_strings = node
        .info_strings
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    let info_strings_display_order = info_strings.keys().cloned().collect::<Vec<_>>();

    out.push(runtime_profile::TRuntimeProfileNode::new(
        node.name.clone(),
        node.children.len() as i32,
        counters,
        i64::from(node.node_id),
        false,
        info_strings,
        info_strings_display_order,
        child_counters_map,
        None,
    ));

    for child in &node.children {
        native_profile_node_to_thrift(child, out)?;
    }
    Ok(())
}

fn native_counter_to_thrift(
    counter: &novarocks::Counter,
) -> Result<runtime_profile::TCounter, String> {
    let unit = unit_from_proto(counter.unit)?;
    Ok(runtime_profile::TCounter::new(
        counter.name.clone(),
        unit,
        counter.value,
        Some(default_counter_strategy(unit)),
        counter.min_value,
        counter.max_value,
    ))
}

fn unit_from_proto(unit: i32) -> Result<metrics::TUnit, String> {
    match novarocks::ProfileUnit::try_from(unit) {
        Ok(novarocks::ProfileUnit::Unit) => Ok(metrics::TUnit::UNIT),
        Ok(novarocks::ProfileUnit::CpuTicks) => Ok(metrics::TUnit::CPU_TICKS),
        Ok(novarocks::ProfileUnit::Bytes) => Ok(metrics::TUnit::BYTES),
        Ok(novarocks::ProfileUnit::TimeNs) => Ok(metrics::TUnit::TIME_NS),
        Ok(novarocks::ProfileUnit::TimeMs) => Ok(metrics::TUnit::TIME_MS),
        Ok(novarocks::ProfileUnit::TimeS) => Ok(metrics::TUnit::TIME_S),
        Ok(novarocks::ProfileUnit::None) => Ok(metrics::TUnit::NONE),
        Ok(novarocks::ProfileUnit::Unspecified) => {
            Err("ProfileUnit is unspecified in native runtime profile".to_string())
        }
        Err(_) => Err(format!(
            "unknown ProfileUnit value {unit} in native runtime profile"
        )),
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
        let min_value = *self.min_value.lock().unwrap_or_else(|e| e.into_inner());
        let max_value = *self.max_value.lock().unwrap_or_else(|e| e.into_inner());
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

#[cfg(test)]
mod tests {
    use super::{
        default_native_counter_strategy, native_profile_tree_to_thrift, CounterAggregateType,
        ProfileUnit, RuntimeProfile, ROOT_COUNTER,
    };
    use crate::proto::novarocks;
    use crate::thrift::metrics;

    #[test]
    fn native_profile_unit_roundtrips_proto_values() {
        assert_eq!(
            ProfileUnit::from_proto(novarocks::ProfileUnit::Unit as i32).expect("UNIT converts"),
            ProfileUnit::Unit
        );
        assert_eq!(
            ProfileUnit::from_proto(novarocks::ProfileUnit::TimeNs as i32)
                .expect("TIME_NS converts"),
            ProfileUnit::TimeNs
        );
        assert_eq!(ProfileUnit::None.to_proto(), novarocks::ProfileUnit::None);
        assert!(
            ProfileUnit::from_proto(novarocks::ProfileUnit::Unspecified as i32).is_err(),
            "unspecified proto unit must not silently become a runtime unit"
        );
    }

    #[test]
    fn native_counter_strategy_defaults_match_existing_merge_behavior() {
        assert_eq!(
            default_native_counter_strategy(ProfileUnit::TimeNs).aggregate_type(),
            CounterAggregateType::Avg
        );
        assert_eq!(
            default_native_counter_strategy(ProfileUnit::TimeMs).aggregate_type(),
            CounterAggregateType::Avg
        );
        assert_eq!(
            default_native_counter_strategy(ProfileUnit::Bytes).aggregate_type(),
            CounterAggregateType::Sum
        );
        assert_eq!(
            default_native_counter_strategy(ProfileUnit::Unit).aggregate_type(),
            CounterAggregateType::Sum
        );
    }

    #[test]
    fn thrift_tree_keeps_child_counter_hierarchy() {
        let profile = RuntimeProfile::new("test");
        profile.counter_add("IOTaskExecTime", metrics::TUnit::TIME_NS, 10);
        profile.counter_add_with_parent(
            "ColumnReadTime",
            metrics::TUnit::TIME_NS,
            5,
            "IOTaskExecTime",
        );
        let _ = profile.add_child_counter("InputStream", metrics::TUnit::NONE, "IOTaskExecTime");
        profile.counter_add_with_parent("AppIOTime", metrics::TUnit::TIME_NS, 3, "InputStream");

        let tree = profile.to_thrift_tree();
        let node = tree.nodes.first().expect("runtime profile node");
        let root_children = node
            .child_counters_map
            .get(ROOT_COUNTER)
            .expect("root children");
        assert!(root_children.contains("IOTaskExecTime"));
        let io_children = node
            .child_counters_map
            .get("IOTaskExecTime")
            .expect("IOTaskExecTime children");
        assert!(io_children.contains("ColumnReadTime"));
        assert!(io_children.contains("InputStream"));
        let input_stream_children = node
            .child_counters_map
            .get("InputStream")
            .expect("InputStream children");
        assert!(input_stream_children.contains("AppIOTime"));
    }

    #[test]
    fn merge_isomorphic_profiles_keeps_counter_parent() {
        let p1 = RuntimeProfile::new("p");
        p1.counter_add("IOTaskExecTime", metrics::TUnit::TIME_NS, 10);
        p1.counter_add_with_parent("OpenFile", metrics::TUnit::TIME_NS, 4, "IOTaskExecTime");

        let p2 = RuntimeProfile::new("p");
        p2.counter_add("IOTaskExecTime", metrics::TUnit::TIME_NS, 12);
        p2.counter_add_with_parent("OpenFile", metrics::TUnit::TIME_NS, 5, "IOTaskExecTime");

        let merged = RuntimeProfile::merge_isomorphic_profiles(&[p1, p2]);
        let tree = merged.to_thrift_tree();
        let node = tree.nodes.first().expect("runtime profile node");
        let io_children = node
            .child_counters_map
            .get("IOTaskExecTime")
            .expect("IOTaskExecTime children");
        assert!(io_children.contains("OpenFile"));
    }

    #[test]
    fn native_profile_tree_to_thrift_reconstructs_flat_tree() {
        let native = novarocks::RuntimeProfileTree {
            root: Some(novarocks::ProfileNode {
                name: "Root".to_string(),
                node_id: 10,
                counters: vec![
                    novarocks::Counter {
                        name: "TotalTime".to_string(),
                        parent_name: String::new(),
                        unit: novarocks::ProfileUnit::TimeNs as i32,
                        value: 100,
                        min_value: Some(90),
                        max_value: Some(110),
                    },
                    novarocks::Counter {
                        name: "ScanTime".to_string(),
                        parent_name: "TotalTime".to_string(),
                        unit: novarocks::ProfileUnit::TimeNs as i32,
                        value: 70,
                        min_value: None,
                        max_value: None,
                    },
                ],
                info_strings: [
                    ("z_key".to_string(), "last".to_string()),
                    ("a_key".to_string(), "first".to_string()),
                ]
                .into_iter()
                .collect(),
                children: vec![novarocks::ProfileNode {
                    name: "Child".to_string(),
                    node_id: 20,
                    counters: vec![novarocks::Counter {
                        name: "RowsRead".to_string(),
                        parent_name: String::new(),
                        unit: novarocks::ProfileUnit::Unit as i32,
                        value: 9,
                        min_value: None,
                        max_value: None,
                    }],
                    info_strings: Default::default(),
                    children: vec![],
                }],
            }),
        };

        let thrift = native_profile_tree_to_thrift(&native).expect("native profile decode");

        assert_eq!(thrift.nodes.len(), 2);
        assert_eq!(thrift.nodes[0].name, "Root");
        assert_eq!(thrift.nodes[0].num_children, 1);
        assert_eq!(thrift.nodes[0].metadata, 10);
        assert_eq!(
            thrift.nodes[0].info_strings_display_order,
            vec!["a_key".to_string(), "z_key".to_string()]
        );
        assert_eq!(
            thrift.nodes[0]
                .child_counters_map
                .get(ROOT_COUNTER)
                .expect("root counters"),
            &["TotalTime".to_string()].into_iter().collect()
        );
        assert_eq!(
            thrift.nodes[0]
                .child_counters_map
                .get("TotalTime")
                .expect("child counters"),
            &["ScanTime".to_string()].into_iter().collect()
        );
        assert_eq!(thrift.nodes[1].name, "Child");
        assert_eq!(thrift.nodes[1].metadata, 20);
    }
}
