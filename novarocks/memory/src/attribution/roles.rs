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

//! Bounded, process-local thread-role labels. Allocator hooks never visit this table.
use super::tls;
use crate::lane::THREAD_ROLE_CAPACITY;
use std::{fmt, sync::Mutex};

/// Role zero is always `unknown`; labels are supplied by process composition.
pub type RoleLabels = [Option<&'static str>; THREAD_ROLE_CAPACITY as usize];

/// A diagnostic role issued only by the process role table. It grants no owner.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ThreadRole(u8);
impl ThreadRole {
    pub const UNKNOWN: Self = Self(0);
    pub const fn index(self) -> usize {
        self.0 as usize
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ThreadRoleError {
    EmptyName,
    DuplicateName,
    CapacityExhausted,
    Frozen,
    ThreadStateUnavailable,
}
impl fmt::Display for ThreadRoleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::EmptyName => "thread role name must not be empty",
            Self::DuplicateName => "thread role name is already registered",
            Self::CapacityExhausted => "thread role table capacity is exhausted",
            Self::Frozen => "thread role table is frozen",
            Self::ThreadStateUnavailable => "current thread attribution state is unavailable",
        })
    }
}
impl std::error::Error for ThreadRoleError {}

struct RoleTable {
    labels: RoleLabels,
    count: usize,
    frozen: bool,
}
impl RoleTable {
    const fn new() -> Self {
        let mut labels = [None; THREAD_ROLE_CAPACITY as usize];
        labels[0] = Some("unknown");
        Self {
            labels,
            count: 1,
            frozen: false,
        }
    }
    fn register(&mut self, label: &'static str) -> Result<ThreadRole, ThreadRoleError> {
        if self.frozen {
            return Err(ThreadRoleError::Frozen);
        }
        if label.is_empty() {
            return Err(ThreadRoleError::EmptyName);
        }
        if self.labels[..self.count].contains(&Some(label)) {
            return Err(ThreadRoleError::DuplicateName);
        }
        if self.count == self.labels.len() {
            return Err(ThreadRoleError::CapacityExhausted);
        }
        let role = ThreadRole(self.count as u8);
        self.labels[self.count] = Some(label);
        self.count += 1;
        Ok(role)
    }
}
static ROLES: Mutex<RoleTable> = Mutex::new(RoleTable::new());

/// Registers one static label during process assembly. No registration fallback
/// exists: duplicate, empty, exhausted and frozen registrations are errors.
pub fn register_thread_role(label: &'static str) -> Result<ThreadRole, ThreadRoleError> {
    ROLES
        .lock()
        .expect("thread role table poisoned")
        .register(label)
}

/// Ends process role assembly. Repeated freezes preserve the same frozen table.
pub fn freeze_thread_roles() {
    ROLES.lock().expect("thread role table poisoned").frozen = true;
}

/// Marks this thread for unbound/shadowed tagged allocations. This writes only
/// destructor-free thread state; allocation hooks never read the label table.
pub fn set_current_thread_role(role: ThreadRole) -> Result<(), ThreadRoleError> {
    if tls::set_thread_role(role.0) {
        Ok(())
    } else {
        Err(ThreadRoleError::ThreadStateUnavailable)
    }
}

/// A fixed-size control-plane sample; unregistered positions remain `None`.
pub fn thread_role_labels() -> RoleLabels {
    ROLES.lock().expect("thread role table poisoned").labels
}

/// Marks the current worker at startup using a role registered by process
/// assembly. Standalone components without role assembly remain unknown.
pub fn enter_registered_thread_role(label: &'static str) {
    let index = ROLES
        .lock()
        .expect("thread role table poisoned")
        .labels
        .iter()
        .position(|name| *name == Some(label));
    if let Some(index) = index {
        set_current_thread_role(ThreadRole(index as u8))
            .expect("worker startup has live thread-local attribution state");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_errors_do_not_mutate_labels_and_freeze_is_explicit() {
        let mut table = RoleTable::new();
        assert_eq!(table.labels[ThreadRole::UNKNOWN.index()], Some("unknown"));
        let original = table.labels;
        assert_eq!(table.register(""), Err(ThreadRoleError::EmptyName));
        assert_eq!(
            table.register("unknown"),
            Err(ThreadRoleError::DuplicateName)
        );
        assert_eq!(table.labels, original);
        let driver = table.register("driver").unwrap();
        assert_eq!(driver.index(), 1);
        let registered = table.labels;
        assert_eq!(
            table.register("driver"),
            Err(ThreadRoleError::DuplicateName)
        );
        assert_eq!(table.labels, registered);
        table.frozen = true;
        assert_eq!(table.register("scan"), Err(ThreadRoleError::Frozen));
        assert_eq!(table.labels, registered);
    }

    #[test]
    fn bounded_capacity_counts_unknown_and_never_reissues_an_id() {
        const LABELS: [&str; 15] = [
            "role1", "role2", "role3", "role4", "role5", "role6", "role7", "role8", "role9",
            "role10", "role11", "role12", "role13", "role14", "role15",
        ];
        let mut table = RoleTable::new();
        assert_eq!(table.labels.len(), LABELS.len() + 1);
        for (index, label) in LABELS.into_iter().enumerate() {
            assert_eq!(table.register(label).unwrap().index(), index + 1);
        }
        let full = table.labels;
        assert_eq!(
            table.register("overflow"),
            Err(ThreadRoleError::CapacityExhausted)
        );
        assert_eq!(table.labels, full);
    }
    #[test]
    fn allocator_hooks_do_not_consult_the_locked_control_plane_table() {
        use crate::attribution::AttributingAllocator;
        use std::alloc::{GlobalAlloc, Layout, System};
        let allocator = AttributingAllocator::new(System);
        let layout = Layout::from_size_align(512, 8).unwrap();
        let _locked_labels = ROLES.lock().expect("thread role table poisoned");
        // Completion while the table is locked is the no-role-table-lock
        // assertion. These hooks use only the TLS role number and lane atomics.
        unsafe {
            let ptr = allocator.alloc(layout);
            assert!(!ptr.is_null());
            allocator.dealloc(ptr, layout);
        }
        assert_eq!(allocator.snapshot().live_bytes, 0);
    }
}
