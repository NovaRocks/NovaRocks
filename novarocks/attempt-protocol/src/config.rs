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

//! Frozen coordination policy supplied by the original FE configuration.
//!
//! Transport limits are a value projection of already validated adapter limits;
//! they neither reserve capacity nor grant Worker preparation positions.

mod dispatch;
mod retry;
pub use dispatch::*;
pub use retry::*;

use std::time::Duration;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrozenTransportLimits {
    pub max_batch_items: usize,
    pub max_batch_encoded_bytes: usize,
    pub max_descriptor_encoded_bytes: usize,
    pub max_query_backend_queued_operations: usize,
    pub max_query_backend_queued_bytes: usize,
    pub max_backend_queued_operations: usize,
    pub max_backend_queued_bytes: usize,
    pub max_tasks_per_participation: usize,
    pub max_active_tasks_per_backend: usize,
    pub frontend_queue_residence: Duration,
}

impl FrozenTransportLimits {
    pub const fn max_operation_queued_bytes(self) -> usize {
        self.max_batch_encoded_bytes
    }
    pub const fn batch_fits(self, items: usize, encoded_bytes: usize) -> bool {
        items > 0 && items <= self.max_batch_items && encoded_bytes <= self.max_batch_encoded_bytes
    }
}

impl FrozenTransportLimits {
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
    pub const fn max_tasks_per_participation(self) -> usize {
        self.max_tasks_per_participation
    }
    pub const fn max_active_tasks_per_backend(self) -> usize {
        self.max_active_tasks_per_backend
    }
    pub const fn frontend_queue_residence(self) -> Duration {
        self.frontend_queue_residence
    }
}
