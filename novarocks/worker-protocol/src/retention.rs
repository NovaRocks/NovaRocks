// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Exact bounded retention indexes; identities are never reconstructed from time.
use crate::lease::MonotonicInstant;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetentionError {
    Capacity,
    Duplicate,
    Unknown,
    Overflow,
}
#[derive(Clone, Debug)]
pub struct RetentionIndex<K: Copy + Ord> {
    records: BTreeMap<K, (MonotonicInstant, u64)>,
    deadlines: BTreeSet<(MonotonicInstant, K)>,
    // Addresses into the same records, never a second retained owner. Protected
    // records remain deadline-indexed but cannot be capacity-eviction victims.
    eligible: BTreeSet<(MonotonicInstant, K)>,
    bytes: u64,
    max_records: usize,
    max_bytes: u64,
}
impl<K: Copy + Ord> RetentionIndex<K> {
    pub fn new(max_records: usize, max_bytes: u64) -> Self {
        Self {
            records: BTreeMap::new(),
            deadlines: BTreeSet::new(),
            eligible: BTreeSet::new(),
            bytes: 0,
            max_records,
            max_bytes,
        }
    }
    pub fn insert(
        &mut self,
        key: K,
        until: MonotonicInstant,
        bytes: u64,
    ) -> Result<(), RetentionError> {
        self.insert_internal(key, until, bytes, None)
    }
    pub fn insert_with_protection(
        &mut self,
        key: K,
        until: MonotonicInstant,
        bytes: u64,
        protected: bool,
    ) -> Result<(), RetentionError> {
        self.insert_internal(key, until, bytes, Some(protected))
    }
    fn insert_internal(
        &mut self,
        key: K,
        until: MonotonicInstant,
        bytes: u64,
        protection: Option<bool>,
    ) -> Result<(), RetentionError> {
        if self.records.contains_key(&key) {
            return Err(RetentionError::Duplicate);
        }
        let total = self
            .bytes
            .checked_add(bytes)
            .ok_or(RetentionError::Overflow)?;
        if self.records.len() == self.max_records || total > self.max_bytes {
            return Err(RetentionError::Capacity);
        }
        self.records.insert(key, (until, bytes));
        self.deadlines.insert((until, key));
        if protection == Some(false) {
            self.eligible.insert((until, key));
        }
        self.bytes = total;
        Ok(())
    }
    pub fn remove(&mut self, key: K) -> Result<u64, RetentionError> {
        let (until, bytes) = self.records.remove(&key).ok_or(RetentionError::Unknown)?;
        assert!(
            self.deadlines.remove(&(until, key)),
            "retention indexes agree"
        );
        self.eligible.remove(&(until, key));
        self.bytes -= bytes;
        Ok(bytes)
    }
    pub fn take_due(&mut self, now: MonotonicInstant) -> Option<K> {
        let (deadline, key) = self.deadlines.first().copied()?;
        if now < deadline {
            return None;
        }
        self.remove(key).expect("deadline has a record");
        Some(key)
    }
    pub fn next_deadline(&self) -> Option<MonotonicInstant> {
        self.deadlines.first().map(|(at, _)| *at)
    }
    pub fn oldest(&self) -> Option<K> {
        self.deadlines.first().map(|(_, key)| *key)
    }
    pub fn oldest_eligible(&self) -> Option<K> {
        self.eligible.first().map(|(_, key)| *key)
    }
    pub fn eligible_len(&self) -> usize {
        self.eligible.len()
    }
    pub fn contains(&self, key: K) -> bool {
        self.records.contains_key(&key)
    }
    pub fn len(&self) -> usize {
        self.records.len()
    }
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    fn at(n: u64) -> MonotonicInstant {
        MonotonicInstant::from_origin(Duration::from_secs(n))
    }
    #[test]
    fn due_order_is_exact_and_rejection_leaves_bytes_unchanged() {
        let mut r = RetentionIndex::new(2, 9);
        r.insert(2, at(5), 4).unwrap();
        r.insert(1, at(5), 5).unwrap();
        assert_eq!(r.insert(3, at(1), 1), Err(RetentionError::Capacity));
        assert_eq!(r.bytes(), 9);
        assert_eq!(r.take_due(at(4)), None);
        assert_eq!(r.take_due(at(5)), Some(1));
        assert_eq!(r.take_due(at(5)), Some(2));
        assert!(r.is_empty());
        assert_eq!(r.bytes(), 0);
    }
}
