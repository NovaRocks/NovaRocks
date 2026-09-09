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

pub const MAX_KEY_BYTES: usize = 8 * 1024;
pub const MAX_VALUE_BYTES: usize = 64 * 1024;
pub const MAX_PAGE_SIZE: usize = 1_000;
pub const MAX_TRANSACTION_OPERATIONS: usize = 10_000;
pub const MAX_TRANSACTION_BYTES: usize = 4 * 1024 * 1024;
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

impl Default for StateStoreLimits {
    fn default() -> Self {
        Self {
            max_key_bytes: MAX_KEY_BYTES,
            max_value_bytes: MAX_VALUE_BYTES,
            max_page_size: MAX_PAGE_SIZE,
            max_transaction_operations: MAX_TRANSACTION_OPERATIONS,
            max_transaction_bytes: MAX_TRANSACTION_BYTES,
            transaction_deadline: DEFAULT_TRANSACTION_DEADLINE,
        }
    }
}
