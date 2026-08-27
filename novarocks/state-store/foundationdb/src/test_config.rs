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

use novarocks_spi::state_store::{
    DEFAULT_TRANSACTION_DEADLINE, MAX_KEY_BYTES, MAX_PAGE_SIZE, MAX_RUNNER_ATTEMPTS,
    MAX_TRANSACTION_BYTES, MAX_TRANSACTION_OPERATIONS, MAX_VALUE_BYTES, StateStoreLimits,
};
use uuid::Uuid;

#[doc(hidden)]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FoundationDbTestLimitOverrides {
    pub max_key_bytes: Option<usize>,
    pub max_value_bytes: Option<usize>,
    pub max_page_size: Option<usize>,
    pub max_transaction_operations: Option<usize>,
    pub max_transaction_bytes: Option<usize>,
    pub transaction_deadline_ms: Option<u64>,
    pub runner_max_attempts: Option<usize>,
}

#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FoundationDbTestProviderConfig {
    Foundationdb {
        cluster_file: std::path::PathBuf,
        keyspace_id: Uuid,
    },
}

#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FoundationDbTestStoreConfig {
    pub cluster_id: String,
    pub limits: FoundationDbTestLimitOverrides,
    pub provider: FoundationDbTestProviderConfig,
}

pub(crate) fn resolve_test_limits(
    overrides: &FoundationDbTestLimitOverrides,
) -> Result<StateStoreLimits, ()> {
    fn tightened(value: Option<usize>, maximum: usize) -> Result<usize, ()> {
        let value = value.unwrap_or(maximum);
        (value > 0 && value <= maximum).then_some(value).ok_or(())
    }
    fn tightened_ms(value: Option<u64>, maximum: u64) -> Result<u64, ()> {
        let value = value.unwrap_or(maximum);
        (value > 0 && value <= maximum).then_some(value).ok_or(())
    }

    Ok(StateStoreLimits {
        max_key_bytes: tightened(overrides.max_key_bytes, MAX_KEY_BYTES)?,
        max_value_bytes: tightened(overrides.max_value_bytes, MAX_VALUE_BYTES)?,
        max_page_size: tightened(overrides.max_page_size, MAX_PAGE_SIZE)?,
        max_transaction_operations: tightened(
            overrides.max_transaction_operations,
            MAX_TRANSACTION_OPERATIONS,
        )?,
        max_transaction_bytes: tightened(overrides.max_transaction_bytes, MAX_TRANSACTION_BYTES)?,
        transaction_deadline: std::time::Duration::from_millis(tightened_ms(
            overrides.transaction_deadline_ms,
            DEFAULT_TRANSACTION_DEADLINE.as_millis() as u64,
        )?),
        runner_max_attempts: tightened(overrides.runner_max_attempts, MAX_RUNNER_ATTEMPTS)?,
    })
}
