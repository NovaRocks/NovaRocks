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

//! Canonical identities retained by actual live source/claim graphs only.
use super::*;
use std::{ops::Deref, sync::Weak};

#[derive(Clone)]
pub(in crate::joint_admission) struct DependencyRef(Arc<DependencyRecord>);
impl Deref for DependencyRef {
    type Target = DependencyRecord;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
pub(super) struct DependencyRegistry {
    canonical: BTreeMap<SharedDependency, Weak<DependencyRecord>>,
    after: Option<SharedDependency>,
    end: Option<SharedDependency>,
    left: usize,
    armed: bool,
    again: bool,
}
impl DependencyRegistry {
    pub(super) fn new() -> Self {
        Self {
            canonical: BTreeMap::new(),
            after: None,
            end: None,
            left: 0,
            armed: true,
            again: false,
        }
    }
    pub(super) fn is_empty(&self) -> bool {
        self.canonical
            .values()
            .all(|record| record.strong_count() == 0)
    }
    pub(super) fn has_zero(&self) -> bool {
        self.armed || self.left != 0 || self.again
    }
    #[cfg(test)]
    pub(super) fn contains(&self, fact: &SharedDependency) -> bool {
        self.canonical
            .get(fact)
            .is_some_and(|record| record.strong_count() != 0)
    }
    pub(super) fn lookup(&mut self, fact: &SharedDependency) -> Option<DependencyRef> {
        self.canonical.get(fact)?.upgrade().map(DependencyRef)
    }
    pub(super) fn insert(
        &mut self,
        fact: SharedDependency,
        ordinal: u64,
    ) -> Result<DependencyRef, WorkError> {
        if let Some(record) = self.lookup(&fact) {
            return Ok(record);
        }
        let record = Arc::new(DependencyRecord {
            fact: fact.clone(),
            ordinal,
        });
        self.canonical.insert(fact, Arc::downgrade(&record));
        Ok(DependencyRef(record))
    }
    pub(super) fn mark_published(&mut self, record: &DependencyRef) {
        debug_assert!(
            self.canonical
                .get(&record.fact)
                .is_some_and(|entry| Weak::ptr_eq(entry, &Arc::downgrade(&record.0)))
        );
    }
    pub(super) fn arm_sweep(&mut self) {
        if self.left == 0 {
            self.armed = true;
        } else {
            self.again = true;
        }
    }
    /// One actual canonical entry per turn. New arrivals do not extend a pass.
    pub(super) fn reap_one(&mut self) -> bool {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        if self.left == 0 {
            if !self.armed && !self.again {
                return false;
            }
            self.armed = false;
            self.again = false;
            self.after = None;
            self.end = self.canonical.last_key_value().map(|(key, _)| key.clone());
            self.left = self.canonical.len();
        }
        let Some(end) = self.end.as_ref() else {
            return false;
        };
        let next = self
            .canonical
            .range((
                self.after.as_ref().map_or(Unbounded, Excluded),
                Included(end),
            ))
            .next()
            .map(|(key, weak)| (key.clone(), weak.strong_count() == 0));
        let Some((key, expired)) = next else {
            self.left = 0;
            return false;
        };
        self.left -= 1;
        self.after = Some(key.clone());
        if expired {
            self.canonical.remove(&key);
        }
        expired
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn canonical_identity_lives_until_its_actual_last_graph_and_can_then_be_reclaimed() {
        let mut registry = DependencyRegistry::new();
        let first = registry.insert(SharedDependency::LocalHost, 1).unwrap();
        let other = registry.lookup(&SharedDependency::LocalHost).unwrap();
        assert_eq!(first.ordinal, other.ordinal);
        drop(first);
        assert!(!registry.reap_one());
        assert_eq!(
            registry
                .lookup(&SharedDependency::LocalHost)
                .unwrap()
                .ordinal,
            1
        );
        drop(other);
        registry.arm_sweep();
        assert!(registry.reap_one());
        assert!(registry.is_empty());
        assert_eq!(
            registry
                .insert(SharedDependency::LocalHost, 2)
                .unwrap()
                .ordinal,
            2
        );
    }
}
