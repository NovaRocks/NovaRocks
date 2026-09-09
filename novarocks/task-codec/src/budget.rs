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

//! Native Task transport payload and queue bounds.
//!
//! These limits are adapter configuration, not execution-domain facts.

use std::time::Duration;

/// Payload and queue budgets of the operation transport.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TransportBudget {
    max_batch_items: usize,
    max_batch_encoded_bytes: usize,
    max_descriptor_encoded_bytes: usize,
    max_query_backend_queued_operations: usize,
    max_query_backend_queued_bytes: usize,
    max_backend_queued_operations: usize,
    max_backend_queued_bytes: usize,
    max_tasks_per_context: usize,
    max_active_tasks_per_backend: usize,
    frontend_queue_residence: Duration,
}

impl TransportBudget {
    pub const DEFAULT: Self = Self {
        max_batch_items: 32,
        max_batch_encoded_bytes: 48 * 1024 * 1024,
        max_descriptor_encoded_bytes: 16 * 1024 * 1024,
        max_query_backend_queued_operations: 4096,
        max_query_backend_queued_bytes: 256 * 1024 * 1024,
        max_backend_queued_operations: 16384,
        max_backend_queued_bytes: 512 * 1024 * 1024,
        max_tasks_per_context: 4096,
        max_active_tasks_per_backend: 32768,
        frontend_queue_residence: Duration::from_secs(15),
    };

    /// Builds a budget, rejecting a zero or an inverted bound.
    ///
    /// The defaults are the frozen contract, but a deployment has to be able
    /// to tighten them and a test has to be able to prove the enforcement
    /// path without manufacturing a 48 MiB payload. The ordering rules are
    /// what make the bounds a hierarchy rather than ten unrelated numbers: a
    /// descriptor has to fit in a batch, a batch in one query's queue, and
    /// that queue in the process's, or the smaller bound makes the larger one
    /// unreachable. The task counts nest for the same reason — a
    /// `QueryContextRef` names one query on one backend, so one context's
    /// tasks are a subset of that backend's.
    #[expect(
        clippy::too_many_arguments,
        reason = "every bound is independent; grouping them would invent a hierarchy the contract does not have"
    )]
    pub fn new(
        max_batch_items: usize,
        max_batch_encoded_bytes: usize,
        max_descriptor_encoded_bytes: usize,
        max_query_backend_queued_operations: usize,
        max_query_backend_queued_bytes: usize,
        max_backend_queued_operations: usize,
        max_backend_queued_bytes: usize,
        max_tasks_per_context: usize,
        max_active_tasks_per_backend: usize,
        frontend_queue_residence: Duration,
    ) -> Option<Self> {
        if max_batch_items == 0
            || max_batch_encoded_bytes == 0
            || max_descriptor_encoded_bytes == 0
            || max_query_backend_queued_operations == 0
            || max_query_backend_queued_bytes == 0
            || max_backend_queued_operations == 0
            || max_backend_queued_bytes == 0
            || max_tasks_per_context == 0
            || max_active_tasks_per_backend == 0
            || frontend_queue_residence.is_zero()
        {
            return None;
        }
        if max_descriptor_encoded_bytes > max_batch_encoded_bytes
            || max_batch_encoded_bytes > max_query_backend_queued_bytes
            || max_query_backend_queued_bytes > max_backend_queued_bytes
            || max_batch_items > max_query_backend_queued_operations
            || max_query_backend_queued_operations > max_backend_queued_operations
            || max_tasks_per_context > max_active_tasks_per_backend
        {
            return None;
        }
        Some(Self {
            max_batch_items,
            max_batch_encoded_bytes,
            max_descriptor_encoded_bytes,
            max_query_backend_queued_operations,
            max_query_backend_queued_bytes,
            max_backend_queued_operations,
            max_backend_queued_bytes,
            max_tasks_per_context,
            max_active_tasks_per_backend,
            frontend_queue_residence,
        })
    }

    pub const fn max_batch_items(self) -> usize {
        self.max_batch_items
    }

    pub const fn max_batch_encoded_bytes(self) -> usize {
        self.max_batch_encoded_bytes
    }

    pub const fn max_descriptor_encoded_bytes(self) -> usize {
        self.max_descriptor_encoded_bytes
    }

    pub const fn max_query_backend_queued_operations(self) -> usize {
        self.max_query_backend_queued_operations
    }

    pub const fn max_query_backend_queued_bytes(self) -> usize {
        self.max_query_backend_queued_bytes
    }

    pub const fn max_backend_queued_operations(self) -> usize {
        self.max_backend_queued_operations
    }

    pub const fn max_backend_queued_bytes(self) -> usize {
        self.max_backend_queued_bytes
    }

    pub const fn max_tasks_per_context(self) -> usize {
        self.max_tasks_per_context
    }

    pub const fn max_active_tasks_per_backend(self) -> usize {
        self.max_active_tasks_per_backend
    }

    /// How long an operation may sit in the frontend queue before it fails
    /// closed locally rather than being sent late.
    pub const fn frontend_queue_residence(self) -> Duration {
        self.frontend_queue_residence
    }

    /// Whether a batch of `items` totalling `encoded_bytes` fits.
    pub const fn batch_fits(self, items: usize, encoded_bytes: usize) -> bool {
        items > 0 && items <= self.max_batch_items && encoded_bytes <= self.max_batch_encoded_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::TransportBudget;

    #[test]
    fn batches_are_nonempty_and_bounded() {
        let budget = TransportBudget::DEFAULT;
        assert!(budget.batch_fits(1, 1));
        assert!(!budget.batch_fits(0, 0));
        assert!(!budget.batch_fits(budget.max_batch_items() + 1, 1));
        assert!(!budget.batch_fits(1, budget.max_batch_encoded_bytes() + 1));
    }
}
