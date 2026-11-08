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

use anyhow::{Result, bail};
use novarocks_state_store_api::{
    DEFAULT_TRANSACTION_DEADLINE, MAX_PAGE_SIZE, StateStoreCapacity, StateStoreLimits,
};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StateStoreLimitOverrides {
    pub max_key_bytes: Option<usize>,
    pub max_value_bytes: Option<usize>,
    pub max_page_size: Option<usize>,
    pub max_transaction_operations: Option<usize>,
    pub max_transaction_bytes: Option<usize>,
    pub transaction_timeout_ms: Option<u64>,
}

pub(crate) fn resolve_state_store_limits(
    overrides: &StateStoreLimitOverrides,
    capacity: StateStoreCapacity,
) -> Result<StateStoreLimits> {
    if capacity.max_key_bytes == 0
        || capacity.max_value_bytes == 0
        || capacity.max_transaction_bytes == 0
        || capacity.max_transaction_operations == 0
    {
        bail!("InvalidStateStoreConfig: provider capacity must be positive");
    }
    let deadline_ms = tightened_u64(
        "transaction_timeout_ms",
        overrides.transaction_timeout_ms,
        DEFAULT_TRANSACTION_DEADLINE.as_millis() as u64,
    )?;
    Ok(StateStoreLimits {
        max_key_bytes: tightened_usize(
            "max_key_bytes",
            overrides.max_key_bytes,
            capacity.max_key_bytes,
        )?,
        max_value_bytes: tightened_usize(
            "max_value_bytes",
            overrides.max_value_bytes,
            capacity.max_value_bytes,
        )?,
        max_page_size: tightened_usize("max_page_size", overrides.max_page_size, MAX_PAGE_SIZE)?,
        max_transaction_operations: tightened_usize(
            "max_transaction_operations",
            overrides.max_transaction_operations,
            capacity.max_transaction_operations,
        )?,
        max_transaction_bytes: tightened_usize(
            "max_transaction_bytes",
            overrides.max_transaction_bytes,
            capacity.max_transaction_bytes,
        )?,
        transaction_deadline: std::time::Duration::from_millis(deadline_ms),
    })
}

fn tightened_usize(name: &str, override_value: Option<usize>, maximum: usize) -> Result<usize> {
    let value = override_value.unwrap_or(maximum);
    if value == 0 || value > maximum {
        bail!("InvalidStateStoreConfig: {name} must be between 1 and {maximum}, got {value}");
    }
    Ok(value)
}

fn tightened_u64(name: &str, override_value: Option<u64>, maximum: u64) -> Result<u64> {
    let value = override_value.unwrap_or(maximum);
    if value == 0 || value > maximum {
        bail!("InvalidStateStoreConfig: {name} must be between 1 and {maximum}, got {value}");
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    const TEST_CAPACITY: StateStoreCapacity =
        StateStoreCapacity::new(8 * 1024, 64 * 1024, 4 * 1024 * 1024, 10_000);

    #[test]
    fn state_store_limits_use_provider_capacity() {
        let limits =
            resolve_state_store_limits(&StateStoreLimitOverrides::default(), TEST_CAPACITY)
                .expect("common limits");

        assert_eq!(limits.max_key_bytes, TEST_CAPACITY.max_key_bytes);
        assert_eq!(limits.max_value_bytes, TEST_CAPACITY.max_value_bytes);
        assert_eq!(limits.max_page_size, MAX_PAGE_SIZE);
        assert_eq!(
            limits.max_transaction_operations,
            TEST_CAPACITY.max_transaction_operations
        );
        assert_eq!(
            limits.max_transaction_bytes,
            TEST_CAPACITY.max_transaction_bytes
        );
        assert_eq!(limits.transaction_deadline, DEFAULT_TRANSACTION_DEADLINE);
    }

    #[test]
    fn state_store_limits_accept_tighter_overrides() {
        let overrides = StateStoreLimitOverrides {
            max_key_bytes: Some(1024),
            max_value_bytes: Some(2048),
            max_page_size: Some(100),
            max_transaction_operations: Some(200),
            max_transaction_bytes: Some(4096),
            transaction_timeout_ms: Some(500),
        };

        let limits = resolve_state_store_limits(&overrides, TEST_CAPACITY).expect("tighter limits");

        assert_eq!(limits.max_key_bytes, 1024);
        assert_eq!(limits.max_value_bytes, 2048);
        assert_eq!(limits.max_page_size, 100);
        assert_eq!(limits.max_transaction_operations, 200);
        assert_eq!(limits.max_transaction_bytes, 4096);
        assert_eq!(limits.transaction_deadline, Duration::from_millis(500));
    }

    #[test]
    fn state_store_limits_reject_zero_and_relaxed_overrides() {
        let invalid = [
            (
                "max_key_bytes",
                StateStoreLimitOverrides {
                    max_key_bytes: Some(0),
                    ..Default::default()
                },
            ),
            (
                "max_key_bytes",
                StateStoreLimitOverrides {
                    max_key_bytes: Some(TEST_CAPACITY.max_key_bytes + 1),
                    ..Default::default()
                },
            ),
            (
                "max_value_bytes",
                StateStoreLimitOverrides {
                    max_value_bytes: Some(0),
                    ..Default::default()
                },
            ),
            (
                "max_value_bytes",
                StateStoreLimitOverrides {
                    max_value_bytes: Some(TEST_CAPACITY.max_value_bytes + 1),
                    ..Default::default()
                },
            ),
            (
                "max_page_size",
                StateStoreLimitOverrides {
                    max_page_size: Some(0),
                    ..Default::default()
                },
            ),
            (
                "max_page_size",
                StateStoreLimitOverrides {
                    max_page_size: Some(MAX_PAGE_SIZE + 1),
                    ..Default::default()
                },
            ),
            (
                "max_transaction_operations",
                StateStoreLimitOverrides {
                    max_transaction_operations: Some(0),
                    ..Default::default()
                },
            ),
            (
                "max_transaction_operations",
                StateStoreLimitOverrides {
                    max_transaction_operations: Some(TEST_CAPACITY.max_transaction_operations + 1),
                    ..Default::default()
                },
            ),
            (
                "max_transaction_bytes",
                StateStoreLimitOverrides {
                    max_transaction_bytes: Some(0),
                    ..Default::default()
                },
            ),
            (
                "max_transaction_bytes",
                StateStoreLimitOverrides {
                    max_transaction_bytes: Some(TEST_CAPACITY.max_transaction_bytes + 1),
                    ..Default::default()
                },
            ),
            (
                "transaction_timeout_ms",
                StateStoreLimitOverrides {
                    transaction_timeout_ms: Some(0),
                    ..Default::default()
                },
            ),
            (
                "transaction_timeout_ms",
                StateStoreLimitOverrides {
                    transaction_timeout_ms: Some(
                        DEFAULT_TRANSACTION_DEADLINE.as_millis() as u64 + 1,
                    ),
                    ..Default::default()
                },
            ),
        ];

        for (field, overrides) in invalid {
            let error = resolve_state_store_limits(&overrides, TEST_CAPACITY)
                .expect_err("zero or relaxed limits must fail closed");
            let message = error.to_string();
            assert!(message.contains("InvalidStateStoreConfig"), "{message}");
            assert!(message.contains(field), "{message}");
        }
    }
    #[test]
    fn sqlite_defaults_come_from_its_complete_descriptor() {
        let capacity = novarocks_state_store_sqlite::SQLITE_STATE_STORE_DESCRIPTOR.capacity;
        let limits = resolve_state_store_limits(&StateStoreLimitOverrides::default(), capacity)
            .expect("SQLite limits");
        assert_eq!(limits, capacity.limits());
        assert!(limits.max_value_bytes > 64 * 1024);
        assert_eq!(limits.max_transaction_bytes, usize::MAX);
        assert_eq!(limits.max_transaction_operations, usize::MAX);
    }
}
