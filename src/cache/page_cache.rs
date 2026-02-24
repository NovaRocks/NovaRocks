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
use bytes::Bytes;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct PageCacheStats {
    pub entries: usize,
    pub size: usize,
    pub capacity: usize,
}

pub trait PageCacheValue {
    fn charge(&self) -> usize;
}

impl PageCacheValue for Bytes {
    fn charge(&self) -> usize {
        self.len().max(1)
    }
}

#[derive(Debug)]
pub struct PageCache<K, V = Bytes> {
    inner: Mutex<CacheInner<K, V>>,
}

impl<K, V> PageCache<K, V>
where
    K: Eq + Hash + Clone,
    V: PageCacheValue,
{
    pub fn new(capacity: usize, evict_probability: u32) -> Self {
        Self {
            inner: Mutex::new(CacheInner::new(capacity, evict_probability)),
        }
    }

    pub fn lookup(&self, key: &K) -> Option<PageHandle<V>> {
        let mut inner = self.inner.lock().expect("page cache lock");
        inner.lookup(key)
    }

    pub fn insert(&self, key: K, value: V) -> bool {
        let charge = value.charge();
        self.insert_with_charge_and_probability(key, value, charge, None)
    }

    pub fn insert_with_charge(&self, key: K, value: V, charge: usize) -> bool {
        self.insert_with_charge_and_probability(key, value, charge, None)
    }

    pub fn insert_with_charge_and_probability(
        &self,
        key: K,
        value: V,
        charge: usize,
        evict_probability: Option<u32>,
    ) -> bool {
        let mut inner = self.inner.lock().expect("page cache lock");
        inner.insert(key, value, charge, evict_probability)
    }

    pub fn set_capacity(&self, capacity: usize) -> bool {
        let mut inner = self.inner.lock().expect("page cache lock");
        inner.set_capacity(capacity)
    }

    pub fn set_evict_probability(&self, evict_probability: u32) {
        let mut inner = self.inner.lock().expect("page cache lock");
        inner.set_evict_probability(evict_probability);
    }

    pub fn stats(&self) -> PageCacheStats {
        let inner = self.inner.lock().expect("page cache lock");
        inner.stats()
    }
}

#[derive(Debug)]
struct CacheInner<K, V> {
    entries: HashMap<K, CacheEntry<K, V>>,
    head: Option<K>,
    tail: Option<K>,
    size: usize,
    capacity: usize,
    evict_probability: u32,
    rng: XorShift64,
}

impl<K, V> CacheInner<K, V>
where
    K: Eq + Hash + Clone,
    V: PageCacheValue,
{
    fn new(capacity: usize, evict_probability: u32) -> Self {
        Self {
            entries: HashMap::new(),
            head: None,
            tail: None,
            size: 0,
            capacity: capacity.max(1),
            evict_probability: evict_probability.min(100),
            rng: XorShift64::seeded(),
        }
    }

    fn lookup(&mut self, key: &K) -> Option<PageHandle<V>> {
        let entry = self.entries.get(key)?;
        let handle = PageHandle::new(ArcPageEntry::clone(&entry.data));
        let key = key.clone();
        self.move_to_tail(&key);
        Some(handle)
    }

    fn insert(&mut self, key: K, value: V, charge: usize, evict_probability: Option<u32>) -> bool {
        if charge == 0 || charge > self.capacity {
            return false;
        }

        if let Some(old_charge) = self.entries.get(&key).map(|entry| entry.charge) {
            if charge > old_charge {
                let needed = charge - old_charge;
                if self.size.saturating_add(needed) > self.capacity
                    && !self.maybe_evict(needed, evict_probability)
                {
                    return false;
                }
            }
            if let Some(entry) = self.entries.get_mut(&key) {
                entry.charge = charge;
                entry.data = ArcPageEntry::new(value);
                self.size = self.size.saturating_sub(old_charge).saturating_add(charge);
                self.move_to_tail(&key);
                return true;
            }
        }

        if self.size.saturating_add(charge) > self.capacity {
            if !self.maybe_evict(charge, evict_probability) {
                return false;
            }
        }

        let entry = CacheEntry {
            data: ArcPageEntry::new(value),
            charge,
            prev: None,
            next: None,
        };
        self.entries.insert(key.clone(), entry);
        self.attach_tail(&key);
        self.size = self.size.saturating_add(charge);
        true
    }

    fn maybe_evict(&mut self, needed: usize, evict_probability: Option<u32>) -> bool {
        let probability = evict_probability.unwrap_or(self.evict_probability).min(100);
        if probability == 0 {
            return false;
        }
        if probability < 100 {
            let roll = self.rng.gen_range(100);
            if roll >= probability {
                return false;
            }
        }
        while self.size.saturating_add(needed) > self.capacity {
            if !self.evict_one() {
                return false;
            }
        }
        true
    }

    fn evict_one(&mut self) -> bool {
        let mut current = self.head.clone();
        while let Some(key) = current {
            let pinned = self
                .entries
                .get(&key)
                .map(|entry| entry.data.is_pinned())
                .unwrap_or(false);
            let next = self.entries.get(&key).and_then(|entry| entry.next.clone());
            if pinned {
                current = next;
                continue;
            }
            return self.remove_entry(&key).is_some();
        }
        false
    }

    fn remove_entry(&mut self, key: &K) -> Option<CacheEntry<K, V>> {
        let (prev, next, charge) = {
            let entry = self.entries.get(key)?;
            (entry.prev.clone(), entry.next.clone(), entry.charge)
        };
        if let Some(prev_key) = prev.as_ref() {
            if let Some(prev_entry) = self.entries.get_mut(prev_key) {
                prev_entry.next = next.clone();
            }
        } else {
            self.head = next.clone();
        }
        if let Some(next_key) = next.as_ref() {
            if let Some(next_entry) = self.entries.get_mut(next_key) {
                next_entry.prev = prev.clone();
            }
        } else {
            self.tail = prev.clone();
        }
        let entry = self.entries.remove(key);
        if entry.is_some() {
            self.size = self.size.saturating_sub(charge);
        }
        entry
    }

    fn attach_tail(&mut self, key: &K) {
        let tail_key = self.tail.clone();
        if let Some(entry) = self.entries.get_mut(key) {
            entry.prev = tail_key.clone();
            entry.next = None;
        }
        if let Some(tail) = tail_key {
            if let Some(entry) = self.entries.get_mut(&tail) {
                entry.next = Some(key.clone());
            }
        } else {
            self.head = Some(key.clone());
        }
        self.tail = Some(key.clone());
    }

    fn move_to_tail(&mut self, key: &K) {
        if self.tail.as_ref() == Some(key) {
            return;
        }
        self.detach(key);
        self.attach_tail(key);
    }

    fn detach(&mut self, key: &K) {
        let (prev, next) = match self.entries.get(key) {
            Some(entry) => (entry.prev.clone(), entry.next.clone()),
            None => return,
        };
        if let Some(prev_key) = prev.as_ref() {
            if let Some(prev_entry) = self.entries.get_mut(prev_key) {
                prev_entry.next = next.clone();
            }
        } else {
            self.head = next.clone();
        }
        if let Some(next_key) = next.as_ref() {
            if let Some(next_entry) = self.entries.get_mut(next_key) {
                next_entry.prev = prev.clone();
            }
        } else {
            self.tail = prev.clone();
        }
        if let Some(entry) = self.entries.get_mut(key) {
            entry.prev = None;
            entry.next = None;
        }
    }

    fn set_capacity(&mut self, capacity: usize) -> bool {
        self.capacity = capacity.max(1);
        while self.size > self.capacity {
            if !self.evict_one() {
                return false;
            }
        }
        true
    }

    fn set_evict_probability(&mut self, evict_probability: u32) {
        self.evict_probability = evict_probability.min(100);
    }

    fn stats(&self) -> PageCacheStats {
        PageCacheStats {
            entries: self.entries.len(),
            size: self.size,
            capacity: self.capacity,
        }
    }
}

#[derive(Debug)]
struct CacheEntry<K, V> {
    data: ArcPageEntry<V>,
    charge: usize,
    prev: Option<K>,
    next: Option<K>,
}

#[derive(Debug)]
struct PageEntryData<V> {
    value: V,
    pin_count: AtomicUsize,
}

impl<V> PageEntryData<V> {
    fn new(value: V) -> Self {
        Self {
            value,
            pin_count: AtomicUsize::new(0),
        }
    }

    fn pin(&self) {
        self.pin_count.fetch_add(1, Ordering::AcqRel);
    }

    fn unpin(&self) {
        let prev = self.pin_count.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(prev > 0, "page cache pin count underflow");
    }

    fn is_pinned(&self) -> bool {
        self.pin_count.load(Ordering::Acquire) > 0
    }
}

#[derive(Debug)]
struct ArcPageEntry<V>(std::sync::Arc<PageEntryData<V>>);

impl<V> ArcPageEntry<V> {
    fn new(value: V) -> Self {
        Self(std::sync::Arc::new(PageEntryData::new(value)))
    }

    fn clone(this: &Self) -> Self {
        Self(std::sync::Arc::clone(&this.0))
    }

    fn is_pinned(&self) -> bool {
        self.0.is_pinned()
    }

    fn value(&self) -> &V {
        &self.0.value
    }

    fn pin(&self) {
        self.0.pin();
    }

    fn unpin(&self) {
        self.0.unpin();
    }
}

#[derive(Debug)]
pub struct PageHandle<V = Bytes> {
    data: ArcPageEntry<V>,
}

impl<V> PageHandle<V> {
    fn new(data: ArcPageEntry<V>) -> Self {
        data.pin();
        Self { data }
    }

    pub fn value(&self) -> &V {
        self.data.value()
    }
}

impl PageHandle<Bytes> {
    pub fn bytes(&self) -> &Bytes {
        self.value()
    }
}

impl<V> Clone for PageHandle<V> {
    fn clone(&self) -> Self {
        PageHandle::new(ArcPageEntry::clone(&self.data))
    }
}

impl<V> Drop for PageHandle<V> {
    fn drop(&mut self) {
        self.data.unpin();
    }
}

#[derive(Debug)]
struct XorShift64 {
    state: AtomicU64,
}

impl XorShift64 {
    fn seeded() -> Self {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let mixed = seed ^ seed.rotate_left(17) ^ seed.rotate_right(23);
        let seed = if mixed == 0 {
            0x9e3779b97f4a7c15
        } else {
            mixed
        };
        Self {
            state: AtomicU64::new(seed),
        }
    }

    fn next_u64(&self) -> u64 {
        let mut x = self.state.load(Ordering::Acquire);
        loop {
            let mut y = x;
            y ^= y << 13;
            y ^= y >> 7;
            y ^= y << 17;
            match self
                .state
                .compare_exchange(x, y, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return y,
                Err(v) => x = v,
            }
        }
    }

    fn gen_range(&self, upper: u32) -> u32 {
        if upper == 0 {
            return 0;
        }
        (self.next_u64() % upper as u64) as u32
    }
}
