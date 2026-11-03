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

//! Worker-owned typed read-attempt inputs after Native decoding.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use novarocks_spi::connector::ConnectorExecutionReadBinding;
use novarocks_spi::connector::read_stack::ConnectorReadSplit;

/// A decoded split accepted for one worker attempt.
///
/// Native ingress extracts the protocol sequence and plan-node fields before
/// constructing this value, so Worker owns no generated transport DTO.
#[derive(Clone, Debug)]
pub struct ReceivedReadSplit {
    sequence_id: u64,
    plan_node_id: i32,
    split: ConnectorReadSplit,
}

impl ReceivedReadSplit {
    pub const fn new(sequence_id: u64, plan_node_id: i32, split: ConnectorReadSplit) -> Self {
        Self {
            sequence_id,
            plan_node_id,
            split,
        }
    }

    pub const fn split(&self) -> &ConnectorReadSplit {
        &self.split
    }
}

impl novarocks_execution::connector::ScheduledSplitFacts for ReceivedReadSplit {
    fn sequence_id(&self) -> u64 {
        self.sequence_id
    }

    fn plan_node_id(&self) -> i32 {
        self.plan_node_id
    }

    fn retained_size_in_bytes(&self) -> u64 {
        self.split.facts().retained_size_in_bytes()
    }
}

/// Provisional read bindings collected while an attempt is decoded.
///
/// Bindings remain invisible until publish, so a split cannot resolve
/// against a partially-decoded attempt.
pub struct TaskReadBindingRegistry {
    entries: Mutex<BTreeMap<i32, ConnectorExecutionReadBinding>>,
    published: AtomicBool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadExecutionRegistrationError {
    StatePoisoned,
    Duplicate { plan_node_id: i32 },
}
impl std::fmt::Display for ReadExecutionRegistrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StatePoisoned => f.write_str("typed read context lock poisoned"),
            Self::Duplicate { plan_node_id } => write!(
                f,
                "duplicate typed read execution for plan node {plan_node_id}"
            ),
        }
    }
}
impl std::error::Error for ReadExecutionRegistrationError {}
impl novarocks_execution_contract::failure::FailureProjection for ReadExecutionRegistrationError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use crate::failure_reason::WorkerFailureReason as R;
        use novarocks_execution_contract::failure::*;
        match self {
            Self::StatePoisoned => FailureFact::new(
                FailureClass::Unexpected,
                R::ReadRegistrationPoisoned.reason(),
            ),
            Self::Duplicate { .. } => FailureFact::new(
                FailureClass::Invariant,
                R::ReadRegistrationDuplicate.reason(),
            ),
        }
    }
}

impl Default for TaskReadBindingRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskReadBindingRegistry {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(BTreeMap::new()),
            published: AtomicBool::new(false),
        }
    }

    pub fn register(
        &self,
        plan_node_id: i32,
        execution: ConnectorExecutionReadBinding,
    ) -> Result<(), ReadExecutionRegistrationError> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| ReadExecutionRegistrationError::StatePoisoned)?;
        match entries.entry(plan_node_id) {
            std::collections::btree_map::Entry::Vacant(slot) => {
                slot.insert(execution);
                Ok(())
            }
            std::collections::btree_map::Entry::Occupied(_) => {
                Err(ReadExecutionRegistrationError::Duplicate { plan_node_id })
            }
        }
    }

    pub fn publish(&self) {
        self.published.store(true, Ordering::Release);
    }

    pub fn resolve(&self, plan_node_id: i32) -> Option<ConnectorExecutionReadBinding> {
        if !self.published.load(Ordering::Acquire) {
            return None;
        }
        self.entries.lock().ok()?.get(&plan_node_id).cloned()
    }
}
