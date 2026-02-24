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
//! Pipeline dependency primitives.
//!
//! Responsibilities:
//! - Defines dependency handles, readiness flags, and dependency-manager bookkeeping.
//! - Used by scheduler and operators to coordinate blocking/unblocking transitions.
//!
//! Key exported interfaces:
//! - Types: `DependencyHandle`, `Dependency`, `DependencyManager`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::exec::pipeline::schedule::observer::{Observable, Observer};
use crate::novarocks_logging::debug;

static NEXT_DEP_MANAGER_ID: AtomicUsize = AtomicUsize::new(1);
static NEXT_DEP_ID: AtomicUsize = AtomicUsize::new(1);

/// Reference-counted handle to one pipeline dependency object.
pub type DependencyHandle = Arc<Dependency>;

/// Dependency primitive used to model blocked/unblocked execution conditions.
pub struct Dependency {
    id: usize,
    name: String,
    ready: AtomicBool,
    observable: Arc<Observable>,
}

impl fmt::Debug for Dependency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Dependency")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("ready", &self.is_ready())
            .finish()
    }
}

impl PartialEq for Dependency {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Dependency {}

impl Dependency {
    fn new(name: String) -> Self {
        Self {
            id: NEXT_DEP_ID.fetch_add(1, Ordering::Relaxed),
            name,
            ready: AtomicBool::new(false),
            observable: Arc::new(Observable::new()),
        }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub fn set_ready(&self) {
        let prev = self.ready.swap(true, Ordering::AcqRel);
        if !prev {
            let notify = self.observable.defer_notify();
            notify.arm();
            if should_log_dep(&self.name) {
                debug!(
                    "Dependency ready: dep_id={} name={} observers={}",
                    self.id,
                    self.name,
                    self.observable.num_observers()
                );
            }
        }
    }

    pub fn set_blocked(&self) {
        self.ready.store(false, Ordering::Release);
    }

    pub fn add_waiter(&self, observer: Observer) {
        let ready_before = self.is_ready();
        if ready_before {
            observer();
            return;
        }
        self.observable.add_observer(observer);
        if should_log_dep(&self.name) {
            debug!(
                "Dependency add_waiter: dep_id={} name={} ready_before={} observers_after={}",
                self.id,
                self.name,
                ready_before,
                self.observable.num_observers()
            );
        }
        if self.is_ready() {
            let notify = self.observable.defer_notify();
            notify.arm();
        }
    }
}

#[derive(Clone)]
/// Registry managing dependency objects for one pipeline build/execution context.
pub struct DependencyManager {
    id: usize,
    deps: Arc<Mutex<HashMap<String, DependencyHandle>>>,
}

impl DependencyManager {
    pub fn new() -> Self {
        Self {
            id: NEXT_DEP_MANAGER_ID.fetch_add(1, Ordering::Relaxed),
            deps: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn get_or_create(&self, name: impl Into<String>) -> DependencyHandle {
        let name = name.into();
        let mut guard = self.deps.lock().expect("dependency manager lock");
        guard
            .entry(name.clone())
            .or_insert_with(|| Arc::new(Dependency::new(name)))
            .clone()
    }

    pub fn mark_ready(&self, name: &str) {
        let dep = self.get_or_create(name.to_string());
        dep.set_ready();
    }

    pub fn set_blocked(&self, name: &str) {
        let dep = self.get_or_create(name.to_string());
        dep.set_blocked();
    }
}

fn should_log_dep(name: &str) -> bool {
    name.starts_with("join_build:")
        || name.starts_with("broadcast_join_build:")
        || name.starts_with("nljoin_build:")
        || name.starts_with("local_exchange:")
        || name.starts_with("local_rf:")
}

impl Default for DependencyManager {
    fn default() -> Self {
        Self::new()
    }
}
