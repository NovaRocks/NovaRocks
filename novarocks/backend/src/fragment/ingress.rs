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

//! What a fragment ingress recovers from the wire before it can be run.
//!
//! Decoding a fragment produces two things this backend keeps: the splits an
//! attempt was given, and the provider-private read executions those splits
//! resolve against. Both outlive any one protocol, so they live here rather
//! than inside whichever owner submits the work.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

/// A split received by this backend after the exact binding codec recovered
/// its provider-private SPI payload. Scheduling retains only sequence and
/// plan-node metadata; duplicate suppression never compares payload bytes.
#[derive(Clone, Debug)]
pub(crate) struct ReceivedReadSplit {
    received: novarocks_proto_codec::connector_read::ReceivedScheduledSplit,
    split: novarocks_spi::connector::read_stack::ConnectorReadSplit,
}

impl ReceivedReadSplit {
    pub(crate) const fn new(
        received: novarocks_proto_codec::connector_read::ReceivedScheduledSplit,
        split: novarocks_spi::connector::read_stack::ConnectorReadSplit,
    ) -> Self {
        Self { received, split }
    }

    pub(crate) const fn split(&self) -> &novarocks_spi::connector::read_stack::ConnectorReadSplit {
        &self.split
    }
}

impl novarocks_execution::connector::ScheduledSplitFacts for ReceivedReadSplit {
    fn sequence_id(&self) -> u64 {
        self.received.sequence_id()
    }

    fn plan_node_id(&self) -> i32 {
        self.received.plan_node_id()
    }

    fn retained_size_in_bytes(&self) -> u64 {
        self.split.facts().retained_size_in_bytes()
    }
}

/// Provisional per-attempt read contexts collected while the fragment plan is
/// decoded. They stay invisible until whoever decoded them calls `publish`, so
/// a split that arrives mid-decode cannot resolve against a binding set that a
/// later refusal is still allowed to roll back.
pub(crate) struct TypedReadAttemptContext {
    entries: Mutex<BTreeMap<i32, crate::connector::ConnectorExecutionReadBinding>>,
    published: AtomicBool,
}

impl TypedReadAttemptContext {
    pub(crate) fn new() -> Self {
        Self {
            entries: Mutex::new(BTreeMap::new()),
            published: AtomicBool::new(false),
        }
    }

    pub(crate) fn register(
        &self,
        plan_node_id: i32,
        execution: crate::connector::ConnectorExecutionReadBinding,
    ) -> Result<(), String> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| "typed read context lock poisoned")?;
        if entries.insert(plan_node_id, execution).is_some() {
            return Err(format!(
                "duplicate typed read execution for plan node {plan_node_id}"
            ));
        }
        Ok(())
    }

    pub(crate) fn publish(&self) {
        self.published.store(true, Ordering::Release);
    }

    pub(crate) fn resolve(
        &self,
        plan_node_id: i32,
    ) -> Option<crate::connector::ConnectorExecutionReadBinding> {
        if !self.published.load(Ordering::Acquire) {
            return None;
        }
        self.entries.lock().ok()?.get(&plan_node_id).cloned()
    }
}

/// Why a fragment could not be recovered from its wire form.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NativeFragmentIngressError {
    message: String,
}

impl NativeFragmentIngressError {
    pub(crate) fn new(error: impl fmt::Display) -> Self {
        Self {
            message: error.to_string(),
        }
    }
}
impl fmt::Display for NativeFragmentIngressError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for NativeFragmentIngressError {}
