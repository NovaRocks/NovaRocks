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

use std::time::Duration;

pub const MAX_PAGE_SIZE: usize = 1_000;
pub const DEFAULT_TRANSACTION_DEADLINE: Duration = Duration::from_secs(4);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateStoreLimits {
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    pub max_page_size: usize,
    pub max_transaction_operations: usize,
    pub max_transaction_bytes: usize,
    /// Ceiling on one physical transaction. It is a storage property and
    /// says nothing about how many attempts an application may make.
    pub transaction_deadline: Duration,
}

// Design: ADR-0224 (docs/adr/ADR-0224-mv-document-capacity-comes-from-real-boundaries.md)

/// Usable logical capacity of one provider's storage representation.
/// `usize::MAX` means the backend has no bound for that dimension.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct StateStoreCapacity {
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    pub max_transaction_bytes: usize,
    pub max_transaction_operations: usize,
}

impl StateStoreCapacity {
    pub const fn new(
        key: usize,
        value: usize,
        transaction_bytes: usize,
        operations: usize,
    ) -> Self {
        assert!(key > 0 && value > 0 && transaction_bytes > 0 && operations > 0);
        Self {
            max_key_bytes: key,
            max_value_bytes: value,
            max_transaction_bytes: transaction_bytes,
            max_transaction_operations: operations,
        }
    }

    pub fn limits(self) -> StateStoreLimits {
        StateStoreLimits {
            max_key_bytes: self.max_key_bytes,
            max_value_bytes: self.max_value_bytes,
            max_page_size: MAX_PAGE_SIZE,
            max_transaction_operations: self.max_transaction_operations,
            max_transaction_bytes: self.max_transaction_bytes,
            transaction_deadline: DEFAULT_TRANSACTION_DEADLINE,
        }
    }

    pub fn accepts(self, limits: &StateStoreLimits) -> bool {
        limits.max_key_bytes > 0
            && limits.max_key_bytes <= self.max_key_bytes
            && limits.max_value_bytes > 0
            && limits.max_value_bytes <= self.max_value_bytes
            && limits.max_transaction_bytes > 0
            && limits.max_transaction_bytes <= self.max_transaction_bytes
            && limits.max_transaction_operations > 0
            && limits.max_transaction_operations <= self.max_transaction_operations
    }
}
