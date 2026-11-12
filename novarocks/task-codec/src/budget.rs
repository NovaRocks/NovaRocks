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

/// Effective unary message limits supplied by role configuration. These are
/// protobuf message lengths; gRPC owns its separate five-byte frame header.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TransportMessageLimits {
    pub ordinary_request_max_bytes: usize,
    pub ordinary_response_max_bytes: usize,
    pub control_request_max_bytes: usize,
    pub control_response_max_bytes: usize,
}

impl TransportMessageLimits {
    pub const DEFAULT: Self = Self {
        ordinary_request_max_bytes: 64 * 1024 * 1024,
        ordinary_response_max_bytes: 64 * 1024 * 1024,
        control_request_max_bytes: 1024 * 1024,
        control_response_max_bytes: 64 * 1024 * 1024,
    };

    pub fn validate(self) -> Option<Self> {
        for bytes in [
            self.ordinary_request_max_bytes,
            self.ordinary_response_max_bytes,
            self.control_request_max_bytes,
            self.control_response_max_bytes,
        ] {
            if bytes == 0 || u32::try_from(bytes).is_err() {
                return None;
            }
        }
        if self.control_request_max_bytes > self.ordinary_request_max_bytes
            || self.control_response_max_bytes > self.ordinary_response_max_bytes
        {
            return None;
        }
        Some(self)
    }

    pub const fn request_max_bytes(self, control: bool) -> usize {
        if control {
            self.control_request_max_bytes
        } else {
            self.ordinary_request_max_bytes
        }
    }
    pub const fn response_max_bytes(self, control: bool) -> usize {
        if control {
            self.control_response_max_bytes
        } else {
            self.ordinary_response_max_bytes
        }
    }
}

/// Request grouping targets and the original queue policy. The FE queue
/// author separately validates its queued-carrier bound against these actual
/// windows before installing transport; encoded bytes are not queued bytes.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TransportBudget {
    max_batch_items: usize,
    batch_target_encoded_bytes: usize,
    message_limits: TransportMessageLimits,
    max_query_backend_queued_operations: usize,
    max_query_backend_queued_bytes: usize,
    max_backend_queued_operations: usize,
    max_backend_queued_bytes: usize,
    max_tasks_per_participation: usize,
    max_active_tasks_per_backend: usize,
    frontend_queue_residence: Duration,
}

impl TransportBudget {
    pub const DEFAULT: Self = Self {
        max_batch_items: 32,
        batch_target_encoded_bytes: 48 * 1024 * 1024,
        message_limits: TransportMessageLimits::DEFAULT,
        max_query_backend_queued_operations: 4096,
        max_query_backend_queued_bytes: 256 * 1024 * 1024,
        max_backend_queued_operations: 16384,
        max_backend_queued_bytes: 512 * 1024 * 1024,
        max_tasks_per_participation: 4096,
        max_active_tasks_per_backend: 32768,
        frontend_queue_residence: Duration::from_secs(15),
    };

    /// Validate the queue policy and message representation. Request-window
    /// consistency additionally consumes the original FE queue author's Q-hat;
    /// it cannot be inferred from a grouping target or message length alone.
    pub fn new(
        max_batch_items: usize,
        batch_target_encoded_bytes: usize,
        message_limits: TransportMessageLimits,
        max_query_backend_queued_operations: usize,
        max_query_backend_queued_bytes: usize,
        max_backend_queued_operations: usize,
        max_backend_queued_bytes: usize,
        max_tasks_per_participation: usize,
        max_active_tasks_per_backend: usize,
        frontend_queue_residence: Duration,
    ) -> Option<Self> {
        message_limits.validate()?;
        if max_batch_items == 0
            || batch_target_encoded_bytes == 0
            || max_query_backend_queued_operations == 0
            || max_query_backend_queued_bytes == 0
            || max_backend_queued_operations == 0
            || max_backend_queued_bytes == 0
            || max_tasks_per_participation == 0
            || max_active_tasks_per_backend == 0
            || frontend_queue_residence.is_zero()
        {
            return None;
        }
        let minimum_process_items = max_batch_items.checked_mul(2)?;
        if max_query_backend_queued_bytes > max_backend_queued_bytes
            || max_batch_items > max_query_backend_queued_operations
            || max_query_backend_queued_operations > max_backend_queued_operations
            || max_tasks_per_participation > max_active_tasks_per_backend
            || minimum_process_items > max_backend_queued_operations
        {
            return None;
        }
        Some(Self {
            max_batch_items,
            batch_target_encoded_bytes,
            message_limits,
            max_query_backend_queued_operations,
            max_query_backend_queued_bytes,
            max_backend_queued_operations,
            max_backend_queued_bytes,
            max_tasks_per_participation,
            max_active_tasks_per_backend,
            frontend_queue_residence,
        })
    }
    pub const fn max_batch_items(self) -> usize {
        self.max_batch_items
    }
    pub const fn batch_target_encoded_bytes(self) -> usize {
        self.batch_target_encoded_bytes
    }
    pub const fn message_limits(self) -> TransportMessageLimits {
        self.message_limits
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
    pub const fn max_tasks_per_participation(self) -> usize {
        self.max_tasks_per_participation
    }
    pub const fn max_active_tasks_per_backend(self) -> usize {
        self.max_active_tasks_per_backend
    }
    pub const fn frontend_queue_residence(self) -> Duration {
        self.frontend_queue_residence
    }

    /// Grouping bytes are a target, not a codec refusal. The actual method
    /// limit is checked by its transport before an operation is reserved.
    pub const fn batch_fits(self, items: usize, _encoded_bytes: usize) -> bool {
        items > 0 && items <= self.max_batch_items
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn grouping_target_does_not_reject_large_operations() {
        let budget = TransportBudget::DEFAULT;
        assert!(budget.batch_fits(1, budget.batch_target_encoded_bytes() + 1));
        assert!(!budget.batch_fits(0, 0));
        assert!(!budget.batch_fits(budget.max_batch_items() + 1, 1));
    }
    #[test]
    fn message_limits_follow_grpc_length_representation() {
        let limits = TransportMessageLimits {
            ordinary_request_max_bytes: 128 * 1024 * 1024,
            ordinary_response_max_bytes: 128 * 1024 * 1024,
            ..TransportMessageLimits::DEFAULT
        };
        assert_eq!(limits.validate(), Some(limits));
        assert!(
            TransportMessageLimits {
                ordinary_request_max_bytes: u32::MAX as usize + 1,
                ..limits
            }
            .validate()
            .is_none()
        );
        assert!(
            TransportMessageLimits {
                control_request_max_bytes: 0,
                ..limits
            }
            .validate()
            .is_none()
        );
    }
}
