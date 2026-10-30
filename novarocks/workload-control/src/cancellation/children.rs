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

//! One shared ordered edge index backed by the original fixed child slots.

use super::{Cancellation, ChildLink};
use crate::{WorkError, WorkId};
use std::{
    collections::BTreeMap,
    ops::Bound::{Excluded, Included},
    sync::{Arc, Mutex, Weak},
};

type ChildIndex = BTreeMap<(usize, WorkId), Weak<Cancellation>>;
pub(crate) type SharedChildIndex = Arc<Mutex<ChildIndex>>;

pub(super) enum Children {
    Dynamic(Vec<ChildLink>),
    Fixed(FixedChildren),
}

impl Default for Children {
    fn default() -> Self {
        Self::Dynamic(Vec::new())
    }
}

pub(super) struct FixedChildren {
    shared: SharedChildIndex,
    parent_slot: usize,
    len: usize,
    reserved: usize,
    limit: usize,
}

impl Children {
    pub(super) fn fixed(shared: SharedChildIndex, parent_slot: usize, limit: usize) -> Self {
        Self::Fixed(FixedChildren {
            shared,
            parent_slot,
            len: 0,
            reserved: 0,
            limit,
        })
    }

    pub(super) fn reserve_fixed(&mut self, count: usize) -> Result<(), WorkError> {
        let Self::Fixed(children) = self else {
            return Err(WorkError::Conflict);
        };
        let total = children
            .len
            .checked_add(children.reserved)
            .and_then(|n| n.checked_add(count))
            .ok_or(WorkError::ArithmeticOverflow)?;
        if total > children.limit {
            return Err(WorkError::Capacity("prepaid parent child backing"));
        }
        children.reserved += count;
        Ok(())
    }

    pub(super) fn release_fixed(&mut self, count: usize) {
        let Self::Fixed(children) = self else {
            panic!("fixed child reservation");
        };
        children.reserved = children
            .reserved
            .checked_sub(count)
            .expect("release each original child reservation once");
    }

    pub(super) fn assert_empty_for_reuse(&self) {
        match self {
            Self::Dynamic(children) => assert!(children.is_empty()),
            Self::Fixed(children) => children.assert_empty_for_reuse(),
        }
    }

    pub(super) fn reset(&mut self) {
        match self {
            Self::Dynamic(children) => children.clear(),
            Self::Fixed(children) => children.assert_empty_for_reuse(),
        }
    }

    pub(super) fn next_after(&self, after: Option<WorkId>) -> Option<Arc<Cancellation>> {
        match self {
            Self::Dynamic(children) => {
                let index = after.map_or(0, |sequence| {
                    children.partition_point(|link| link.sequence <= sequence)
                });
                children[index..]
                    .iter()
                    .find_map(|link| link.child.upgrade())
            }
            Self::Fixed(children) => children.next_after(after),
        }
    }

    pub(super) fn detach_exact(&mut self, sequence: WorkId, child: &Weak<Cancellation>) {
        match self {
            Self::Dynamic(children) => children
                .retain(|link| link.sequence != sequence || !Weak::ptr_eq(&link.child, child)),
            Self::Fixed(children) => children.detach_exact(sequence, child),
        }
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Dynamic(children) => children.len(),
            Self::Fixed(children) => children.len,
        }
    }

    #[cfg(test)]
    pub(super) fn fixed_facts(&self) -> (usize, *const Mutex<ChildIndex>, usize) {
        let Self::Fixed(children) = self else {
            panic!("expected fixed child index")
        };
        (
            children.limit,
            Arc::as_ptr(&children.shared),
            children.shared.lock().unwrap().len(),
        )
    }
}

impl FixedChildren {
    // All operations on one prefix hold its original Cancellation state lock.
    // The shared index never obtains a Cancellation or WorkloadControl lock.
    pub(super) fn preflight(&self, child: &Children, sequence: WorkId) -> Result<(), WorkError> {
        let Children::Fixed(child) = child else {
            return Err(WorkError::Conflict);
        };
        if !Arc::ptr_eq(&self.shared, &child.shared)
            || self.parent_slot == child.parent_slot
            || child.len != 0
        {
            return Err(WorkError::Conflict);
        }
        if self.len + self.reserved == self.limit {
            return Err(WorkError::Capacity("prepaid parent child backing"));
        }
        if self
            .shared
            .lock()
            .unwrap()
            .contains_key(&(self.parent_slot, sequence))
        {
            return Err(WorkError::Conflict);
        }
        Ok(())
    }

    // The original child slot and unique anchor were obtained before mutation.
    // ParentLink is installed before this weak row; the same parent lock spans
    // preflight and insertion. There is no second capacity authority or queue.
    pub(super) fn insert_preflighted(&mut self, sequence: WorkId, child: Weak<Cancellation>) {
        assert!(self.len + self.reserved < self.limit);
        let old = self
            .shared
            .lock()
            .unwrap()
            .insert((self.parent_slot, sequence), child);
        assert!(old.is_none(), "checked child sequence is unique");
        self.len = self.len.checked_add(1).expect("bounded child count");
    }

    fn next_after(&self, after: Option<WorkId>) -> Option<Arc<Cancellation>> {
        let lower = after.map_or(Included((self.parent_slot, WorkId(0))), |sequence| {
            Excluded((self.parent_slot, sequence))
        });
        let upper = Included((self.parent_slot, WorkId(u64::MAX)));
        self.shared
            .lock()
            .unwrap()
            .range((lower, upper))
            .find_map(|(_, child)| child.upgrade())
    }

    fn detach_exact(&mut self, sequence: WorkId, child: &Weak<Cancellation>) {
        let key = (self.parent_slot, sequence);
        let mut index = self.shared.lock().unwrap();
        if index
            .get(&key)
            .is_some_and(|original| Weak::ptr_eq(original, child))
        {
            index.remove(&key);
            self.len = self.len.checked_sub(1).expect("registered child count");
        }
    }

    fn assert_empty_for_reuse(&self) {
        assert_eq!(self.len, 0);
        assert_eq!(self.reserved, 0);
        assert!(
            self.shared
                .lock()
                .unwrap()
                .range((
                    Included((self.parent_slot, WorkId(0))),
                    Included((self.parent_slot, WorkId(u64::MAX))),
                ))
                .next()
                .is_none(),
            "reused parent has no old weak edges"
        );
    }
}

pub(crate) fn new_shared_child_index() -> SharedChildIndex {
    let shared = Arc::new(Mutex::new(BTreeMap::new()));
    drop(shared.lock().unwrap());
    shared
}

pub(crate) fn shared_child_index_backing_bytes(count: usize) -> Result<usize, WorkError> {
    use crate::control_storage::{arc_bytes, btree_peak_bytes};
    // N original anchors admit at most N-1 parent edges, including retired
    // anchors retained by views/DFS. BTree nodes grow on insertion within this
    // prepaid peak; this is not a claim that every node is preallocated.
    arc_bytes::<Mutex<ChildIndex>>()?
        .checked_add(btree_peak_bytes::<(usize, WorkId), Weak<Cancellation>>(
            count.saturating_sub(1),
        )?)
        .and_then(|n| n.checked_add(if cfg!(target_os = "macos") { 64 } else { 0 }))
        .ok_or(WorkError::ArithmeticOverflow)
}
