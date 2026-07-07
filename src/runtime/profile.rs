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

#[derive(Clone, Debug)]
struct CounterSnapshot {
    name: String,
    parent_name: String,
    unit: ProfileUnit,
    strategy: CounterStrategy,
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
pub enum CounterMergeType {
    MergeAll,
    SkipAll,
    SkipFirstMerge,
    SkipSecondMerge,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CounterMinMaxType {
    MinMaxAll,
    SkipAll,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CounterStrategy {
    aggregate_type: CounterAggregateType,
    merge_type: CounterMergeType,
    display_threshold: i64,
    min_max_type: Option<CounterMinMaxType>,
}

impl CounterStrategy {
    pub fn new(aggregate_type: CounterAggregateType) -> Self {
        Self {
            aggregate_type,
            merge_type: CounterMergeType::MergeAll,
            display_threshold: 0,
            min_max_type: Some(CounterMinMaxType::MinMaxAll),
        }
    }

    pub fn custom(
        aggregate_type: CounterAggregateType,
        merge_type: CounterMergeType,
        display_threshold: i64,
        min_max_type: Option<CounterMinMaxType>,
    ) -> Self {
        Self {
            aggregate_type,
            merge_type,
            display_threshold,
            min_max_type,
        }
    }

    pub fn aggregate_type(self) -> CounterAggregateType {
        self.aggregate_type
    }

    pub fn merge_type(self) -> CounterMergeType {
        self.merge_type
    }

    pub fn display_threshold(self) -> i64 {
        self.display_threshold
    }

    pub fn min_max_type(self) -> Option<CounterMinMaxType> {
        self.min_max_type
    }
}

pub fn default_counter_strategy(unit: ProfileUnit) -> CounterStrategy {
    let aggregate_type = match unit {
        ProfileUnit::CpuTicks | ProfileUnit::TimeNs | ProfileUnit::TimeMs | ProfileUnit::TimeS => {
            CounterAggregateType::Avg
        }
        ProfileUnit::Unit | ProfileUnit::Bytes | ProfileUnit::None => CounterAggregateType::Sum,
    };
    CounterStrategy::new(aggregate_type)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeProfileTree {
    pub root: ProfileNode,
}

impl RuntimeProfileTree {
    pub(crate) fn to_proto(&self) -> novarocks::RuntimeProfileTree {
        novarocks::RuntimeProfileTree {
            root: Some(self.root.to_proto()),
        }
    }

    pub(crate) fn from_proto(tree: &novarocks::RuntimeProfileTree) -> Result<Self, String> {
        let root = tree
            .root
            .as_ref()
            .ok_or_else(|| "RuntimeProfileTree missing root".to_string())?;
        Ok(Self {
            root: ProfileNode::from_proto(root)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProfileNode {
    pub name: String,
    pub node_id: i32,
    pub counters: Vec<ProfileCounter>,
    pub info_strings: BTreeMap<String, String>,
    pub children: Vec<ProfileNode>,
}

impl ProfileNode {
    fn to_proto(&self) -> novarocks::ProfileNode {
        novarocks::ProfileNode {
            name: self.name.clone(),
            node_id: self.node_id,
            counters: self.counters.iter().map(ProfileCounter::to_proto).collect(),
            info_strings: self.info_strings.clone().into_iter().collect(),
            children: self.children.iter().map(ProfileNode::to_proto).collect(),
        }
    }

    fn from_proto(node: &novarocks::ProfileNode) -> Result<Self, String> {
        Ok(Self {
            name: node.name.clone(),
            node_id: node.node_id,
            counters: node
                .counters
                .iter()
                .map(ProfileCounter::from_proto)
                .collect::<Result<Vec<_>, _>>()?,
            info_strings: node
                .info_strings
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            children: node
                .children
                .iter()
                .map(ProfileNode::from_proto)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProfileCounter {
    pub name: String,
    pub parent_name: String,
    pub unit: ProfileUnit,
    pub strategy: CounterStrategy,
    pub value: i64,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
}

impl ProfileCounter {
    fn to_proto(&self) -> novarocks::Counter {
        novarocks::Counter {
            name: self.name.clone(),
            parent_name: self.parent_name.clone(),
            unit: self.unit.to_proto() as i32,
            value: self.value,
            min_value: self.min_value,
            max_value: self.max_value,
        }
    }

    fn from_proto(counter: &novarocks::Counter) -> Result<Self, String> {
        let unit = ProfileUnit::from_proto(counter.unit)?;
        Ok(Self {
            name: counter.name.clone(),
            parent_name: counter.parent_name.clone(),
            unit,
            strategy: default_counter_strategy(unit),
            value: counter.value,
            min_value: counter.min_value,
            max_value: counter.max_value,
        })
    }
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

    pub fn add_counter(&self, name: impl Into<String>, unit: ProfileUnit) -> CounterRef {
        self.add_counter_with_parent_and_strategy(
            name,
            unit,
            default_counter_strategy(unit),
            ROOT_COUNTER,
        )
    }

    pub fn add_unit_counter(&self, name: impl Into<String>) -> CounterRef {
        self.add_counter(name, ProfileUnit::Unit)
    }

    pub fn add_bytes_counter(&self, name: impl Into<String>) -> CounterRef {
        self.add_counter(name, ProfileUnit::Bytes)
    }

    pub fn add_child_counter(
        &self,
        name: impl Into<String>,
        unit: ProfileUnit,
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
        unit: ProfileUnit,
        strategy: CounterStrategy,
    ) -> CounterRef {
        self.add_counter_with_parent_and_strategy(name, unit, strategy, ROOT_COUNTER)
    }

    pub fn add_counter_with_parent_and_strategy(
        &self,
        name: impl Into<String>,
        unit: ProfileUnit,
        strategy: CounterStrategy,
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

    pub fn counter_add(&self, name: &str, unit: ProfileUnit, delta: i64) {
        let c = self.add_counter(name.to_string(), unit);
        c.add(delta);
    }

    pub fn counter_add_unit(&self, name: &str, delta: i64) {
        self.counter_add(name, ProfileUnit::Unit, delta);
    }

    pub fn counter_add_bytes(&self, name: &str, delta: i64) {
        self.counter_add(name, ProfileUnit::Bytes, delta);
    }

    pub fn counter_add_with_parent(
        &self,
        name: &str,
        unit: ProfileUnit,
        delta: i64,
        parent_name: &str,
    ) {
        let c = self.add_child_counter(name.to_string(), unit, parent_name.to_string());
        c.add(delta);
    }

    pub fn counter_set(&self, name: &str, unit: ProfileUnit, value: i64) {
        let c = self.add_counter(name.to_string(), unit);
        c.set(value);
    }

    pub fn counter_set_unit(&self, name: &str, value: i64) {
        self.counter_set(name, ProfileUnit::Unit, value);
    }

    pub fn counter_set_bytes(&self, name: &str, value: i64) {
        self.counter_set(name, ProfileUnit::Bytes, value);
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
        self.add_counter(name, ProfileUnit::TimeNs)
    }

    pub fn add_child_timer(
        &self,
        name: impl Into<String>,
        parent_name: impl Into<String>,
    ) -> CounterRef {
        self.add_child_counter(name, ProfileUnit::TimeNs, parent_name)
    }

    pub fn scoped_timer(&self, name: impl Into<String>) -> ScopedTimer {
        let counter = self.add_timer(name);
        ScopedTimer::new(counter)
    }

    pub fn to_native_tree(&self) -> RuntimeProfileTree {
        RuntimeProfileTree {
            root: self.to_native_node(),
        }
    }

    pub(crate) fn to_proto(&self) -> novarocks::RuntimeProfileTree {
        self.to_native_tree().to_proto()
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
            let strategy = snapshots[0].strategy;
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

    fn to_native_node(&self) -> ProfileNode {
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
            .map(counter_snapshot_to_native)
            .collect();
        let children = self
            .children()
            .into_iter()
            .map(|child| child.to_native_node())
            .collect();

        ProfileNode {
            name: self.name(),
            node_id: metadata_to_proto_node_id(self.metadata()),
            counters,
            info_strings,
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
            strategy: c.strategy,
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
                    strategy: c.strategy,
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

fn counter_snapshot_to_native(snapshot: CounterSnapshot) -> ProfileCounter {
    ProfileCounter {
        name: snapshot.name,
        parent_name: snapshot.parent_name,
        unit: snapshot.unit,
        strategy: snapshot.strategy,
        value: snapshot.value,
        min_value: snapshot.min_value,
        max_value: snapshot.max_value,
    }
}

pub type CounterRef = Arc<Counter>;

#[derive(Debug)]
pub struct Counter {
    name: String,
    unit: ProfileUnit,
    strategy: CounterStrategy,
    value: AtomicI64,
    min_value: Mutex<Option<i64>>,
    max_value: Mutex<Option<i64>>,
}

impl Counter {
    pub fn new(name: impl Into<String>, unit: ProfileUnit, strategy: CounterStrategy) -> Self {
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
    common.counter_set("CurrentMemoryBytes", ProfileUnit::Bytes, tracker.current());
    common.counter_set("PeakMemoryBytes", ProfileUnit::Bytes, tracker.peak());
    common.counter_set(
        "AllocatedMemoryBytes",
        ProfileUnit::Bytes,
        tracker.allocated(),
    );
    common.counter_set(
        "DeallocatedMemoryBytes",
        ProfileUnit::Bytes,
        tracker.deallocated(),
    );
    let _ = profile.child("UniqueMetrics");
    for child in tracker.children() {
        let child_profile = profile.child(child.label().to_string());
        fill_mem_tracker_profile(&child_profile, &child);
    }
}

pub fn clamp_u128_to_i64(value: u128) -> i64 {
    if value > i64::MAX as u128 {
        i64::MAX
    } else {
        value as i64
    }
}

fn merge_counter_values(strategy: &CounterStrategy, values: &[i64]) -> (i64, i64, i64) {
    let min_value = values.iter().copied().min().unwrap_or(0);
    let max_value = values.iter().copied().max().unwrap_or(0);
    let n = i64::try_from(values.len()).unwrap_or(i64::MAX);
    let sum = values
        .iter()
        .copied()
        .fold(0i64, |acc, v| acc.saturating_add(v));
    let avg = if n <= 0 { 0 } else { sum / n };
    let value = match strategy.aggregate_type() {
        CounterAggregateType::Sum => sum,
        CounterAggregateType::Avg => avg,
        CounterAggregateType::SumAvg => sum,
        CounterAggregateType::AvgSum => avg,
    };
    (value, min_value, max_value)
}

#[cfg(test)]
mod tests {
    use super::{
        CounterAggregateType, CounterMergeType, CounterMinMaxType, CounterStrategy, ProfileUnit,
        ROOT_COUNTER, RuntimeProfile, RuntimeProfileTree, default_counter_strategy,
    };
    use crate::proto::novarocks;

    #[test]
    fn native_profile_unit_roundtrips_proto_values() {
        let cases = [
            (ProfileUnit::Unit, novarocks::ProfileUnit::Unit),
            (ProfileUnit::CpuTicks, novarocks::ProfileUnit::CpuTicks),
            (ProfileUnit::Bytes, novarocks::ProfileUnit::Bytes),
            (ProfileUnit::TimeNs, novarocks::ProfileUnit::TimeNs),
            (ProfileUnit::TimeMs, novarocks::ProfileUnit::TimeMs),
            (ProfileUnit::TimeS, novarocks::ProfileUnit::TimeS),
            (ProfileUnit::None, novarocks::ProfileUnit::None),
        ];
        for (unit, proto) in cases {
            assert_eq!(
                ProfileUnit::from_proto(proto as i32).expect("valid unit converts"),
                unit
            );
            assert_eq!(unit.to_proto(), proto);
        }
        assert!(
            ProfileUnit::from_proto(novarocks::ProfileUnit::Unspecified as i32).is_err(),
            "unspecified proto unit must not silently become a runtime unit"
        );
    }

    #[test]
    fn native_counter_strategy_defaults_match_existing_merge_behavior() {
        let cases = [
            (ProfileUnit::Unit, CounterAggregateType::Sum),
            (ProfileUnit::CpuTicks, CounterAggregateType::Avg),
            (ProfileUnit::Bytes, CounterAggregateType::Sum),
            (ProfileUnit::TimeNs, CounterAggregateType::Avg),
            (ProfileUnit::TimeMs, CounterAggregateType::Avg),
            (ProfileUnit::TimeS, CounterAggregateType::Avg),
            (ProfileUnit::None, CounterAggregateType::Sum),
        ];
        for (native_unit, aggregate_type) in cases {
            let strategy = default_counter_strategy(native_unit);
            assert_eq!(
                strategy.aggregate_type(),
                aggregate_type,
                "native aggregate strategy for {native_unit:?} must match existing merge behavior"
            );
            assert_eq!(strategy.merge_type(), CounterMergeType::MergeAll);
            assert_eq!(strategy.display_threshold(), 0);
            assert_eq!(strategy.min_max_type(), Some(CounterMinMaxType::MinMaxAll));
        }
    }

    #[test]
    fn runtime_profile_counters_use_native_units() {
        let profile = RuntimeProfile::new("native-profile");
        profile.counter_set("RowsRead", ProfileUnit::Unit, 7);
        profile.counter_set("ScanTime", ProfileUnit::TimeNs, 11);

        let rows = profile
            .counter_snapshot("RowsRead")
            .expect("RowsRead counter");
        let scan = profile
            .counter_snapshot("ScanTime")
            .expect("ScanTime counter");

        assert_eq!(rows.unit, ProfileUnit::Unit);
        assert_eq!(scan.unit, ProfileUnit::TimeNs);
        assert_eq!(rows.strategy.aggregate_type(), CounterAggregateType::Sum);
        assert_eq!(scan.strategy.aggregate_type(), CounterAggregateType::Avg);
    }

    #[test]
    fn runtime_profile_builds_native_tree_with_recursive_children() {
        let profile = RuntimeProfile::new("Root");
        profile.set_metadata(10);
        profile.counter_set("RowsRead", ProfileUnit::Unit, 3);
        let child = profile.child("Child");
        child.set_metadata(20);
        child.counter_set("ScanTime", ProfileUnit::TimeNs, 5);

        let tree = profile.to_native_tree();

        assert_eq!(tree.root.name, "Root");
        assert_eq!(tree.root.node_id, 10);
        assert_eq!(tree.root.counters[0].unit, ProfileUnit::Unit);
        assert_eq!(tree.root.children.len(), 1);
        assert_eq!(tree.root.children[0].name, "Child");
        assert_eq!(tree.root.children[0].node_id, 20);
        assert_eq!(tree.root.children[0].counters[0].unit, ProfileUnit::TimeNs);
    }

    #[test]
    fn native_profile_tree_from_proto_rebuilds_default_strategy() {
        let profile = RuntimeProfile::new("Root");
        profile.counter_set("ScanTime", ProfileUnit::TimeNs, 5);
        let proto = profile.to_native_tree().to_proto();

        let tree = RuntimeProfileTree::from_proto(&proto).expect("native profile proto decodes");
        let counter = tree
            .root
            .counters
            .iter()
            .find(|counter| counter.name == "ScanTime")
            .expect("ScanTime counter");

        assert_eq!(counter.unit, ProfileUnit::TimeNs);
        assert_eq!(counter.strategy.aggregate_type(), CounterAggregateType::Avg);
    }

    #[test]
    fn native_tree_preserves_counter_strategy_fields() {
        let profile = RuntimeProfile::new("strategy-profile");
        let counter = profile.add_counter_with_strategy(
            "CustomCounter",
            ProfileUnit::TimeMs,
            CounterStrategy::custom(
                CounterAggregateType::SumAvg,
                CounterMergeType::SkipFirstMerge,
                42,
                Some(CounterMinMaxType::SkipAll),
            ),
        );
        counter.set(99);
        counter.set_min(7);
        counter.set_max(123);

        let tree = profile.to_native_tree();
        let native_counter = tree
            .root
            .counters
            .iter()
            .find(|counter| counter.name == "CustomCounter")
            .expect("custom counter");

        assert_eq!(native_counter.unit, ProfileUnit::TimeMs);
        assert_eq!(native_counter.value, 99);
        assert_eq!(native_counter.min_value, Some(7));
        assert_eq!(native_counter.max_value, Some(123));
        assert_eq!(
            native_counter.strategy.aggregate_type(),
            CounterAggregateType::SumAvg
        );
        assert_eq!(
            native_counter.strategy.merge_type(),
            CounterMergeType::SkipFirstMerge
        );
        assert_eq!(native_counter.strategy.display_threshold(), 42);
        assert_eq!(
            native_counter.strategy.min_max_type(),
            Some(CounterMinMaxType::SkipAll)
        );
    }

    #[test]
    fn native_tree_keeps_child_counter_hierarchy() {
        let profile = RuntimeProfile::new("test");
        profile.counter_add("IOTaskExecTime", ProfileUnit::TimeNs, 10);
        profile.counter_add_with_parent("ColumnReadTime", ProfileUnit::TimeNs, 5, "IOTaskExecTime");
        let _ = profile.add_child_counter("InputStream", ProfileUnit::None, "IOTaskExecTime");
        profile.counter_add_with_parent("AppIOTime", ProfileUnit::TimeNs, 3, "InputStream");

        let tree = profile.to_native_tree();
        let parent_name = |name: &str| {
            tree.root
                .counters
                .iter()
                .find(|counter| counter.name == name)
                .map(|counter| counter.parent_name.as_str())
        };

        assert_eq!(parent_name("IOTaskExecTime"), Some(ROOT_COUNTER));
        assert_eq!(parent_name("ColumnReadTime"), Some("IOTaskExecTime"));
        assert_eq!(parent_name("InputStream"), Some("IOTaskExecTime"));
        assert_eq!(parent_name("AppIOTime"), Some("InputStream"));
    }

    #[test]
    fn merge_isomorphic_profiles_keeps_counter_parent() {
        let p1 = RuntimeProfile::new("p");
        p1.counter_add("IOTaskExecTime", ProfileUnit::TimeNs, 10);
        p1.counter_add_with_parent("OpenFile", ProfileUnit::TimeNs, 4, "IOTaskExecTime");

        let p2 = RuntimeProfile::new("p");
        p2.counter_add("IOTaskExecTime", ProfileUnit::TimeNs, 12);
        p2.counter_add_with_parent("OpenFile", ProfileUnit::TimeNs, 5, "IOTaskExecTime");

        let merged = RuntimeProfile::merge_isomorphic_profiles(&[p1, p2]);
        let tree = merged.to_native_tree();
        let open_file = tree
            .root
            .counters
            .iter()
            .find(|counter| counter.name == "OpenFile")
            .expect("OpenFile counter");
        assert_eq!(open_file.parent_name, "IOTaskExecTime");
    }
}
