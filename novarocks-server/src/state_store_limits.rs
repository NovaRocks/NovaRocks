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
use novarocks_spi::state_store::{
    DEFAULT_TRANSACTION_DEADLINE, MAX_KEY_BYTES, MAX_PAGE_SIZE, MAX_RUNNER_ATTEMPTS,
    MAX_TRANSACTION_BYTES, MAX_TRANSACTION_OPERATIONS, MAX_VALUE_BYTES, StateStoreLimits,
};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StateStoreLimitOverrides {
    pub max_key_bytes: Option<usize>,
    pub max_value_bytes: Option<usize>,
    pub max_page_size: Option<usize>,
    pub max_transaction_operations: Option<usize>,
    pub max_transaction_bytes: Option<usize>,
    pub transaction_deadline_ms: Option<u64>,
    pub runner_max_attempts: Option<usize>,
}

pub(crate) fn resolve_state_store_limits(
    overrides: &StateStoreLimitOverrides,
    provider_max_key_bytes: usize,
) -> Result<StateStoreLimits> {
    if provider_max_key_bytes == 0 || provider_max_key_bytes > MAX_KEY_BYTES {
        bail!(
            "InvalidStateStoreConfig: provider_max_key_bytes must be between 1 and {MAX_KEY_BYTES}, got {provider_max_key_bytes}"
        );
    }
    let deadline_ms = tightened_u64(
        "transaction_deadline_ms",
        overrides.transaction_deadline_ms,
        DEFAULT_TRANSACTION_DEADLINE.as_millis() as u64,
    )?;
    Ok(StateStoreLimits {
        max_key_bytes: tightened_usize(
            "max_key_bytes",
            overrides.max_key_bytes,
            provider_max_key_bytes,
        )?,
        max_value_bytes: tightened_usize(
            "max_value_bytes",
            overrides.max_value_bytes,
            MAX_VALUE_BYTES,
        )?,
        max_page_size: tightened_usize("max_page_size", overrides.max_page_size, MAX_PAGE_SIZE)?,
        max_transaction_operations: tightened_usize(
            "max_transaction_operations",
            overrides.max_transaction_operations,
            MAX_TRANSACTION_OPERATIONS,
        )?,
        max_transaction_bytes: tightened_usize(
            "max_transaction_bytes",
            overrides.max_transaction_bytes,
            MAX_TRANSACTION_BYTES,
        )?,
        transaction_deadline: std::time::Duration::from_millis(deadline_ms),
        runner_max_attempts: tightened_usize(
            "runner_max_attempts",
            overrides.runner_max_attempts,
            MAX_RUNNER_ATTEMPTS,
        )?,
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

    #[test]
    fn state_store_limits_use_fixed_contract_defaults() {
        let limits =
            resolve_state_store_limits(&StateStoreLimitOverrides::default(), MAX_KEY_BYTES)
                .expect("common limits");

        assert_eq!(limits.max_key_bytes, MAX_KEY_BYTES);
        assert_eq!(limits.max_value_bytes, MAX_VALUE_BYTES);
        assert_eq!(limits.max_page_size, MAX_PAGE_SIZE);
        assert_eq!(
            limits.max_transaction_operations,
            MAX_TRANSACTION_OPERATIONS
        );
        assert_eq!(limits.max_transaction_bytes, MAX_TRANSACTION_BYTES);
        assert_eq!(limits.transaction_deadline, DEFAULT_TRANSACTION_DEADLINE);
        assert_eq!(limits.runner_max_attempts, MAX_RUNNER_ATTEMPTS);
    }

    #[test]
    fn state_store_limits_accept_tighter_overrides() {
        let overrides = StateStoreLimitOverrides {
            max_key_bytes: Some(1024),
            max_value_bytes: Some(2048),
            max_page_size: Some(100),
            max_transaction_operations: Some(200),
            max_transaction_bytes: Some(4096),
            transaction_deadline_ms: Some(500),
            runner_max_attempts: Some(2),
        };

        let limits = resolve_state_store_limits(&overrides, MAX_KEY_BYTES).expect("tighter limits");

        assert_eq!(limits.max_key_bytes, 1024);
        assert_eq!(limits.max_value_bytes, 2048);
        assert_eq!(limits.max_page_size, 100);
        assert_eq!(limits.max_transaction_operations, 200);
        assert_eq!(limits.max_transaction_bytes, 4096);
        assert_eq!(limits.transaction_deadline, Duration::from_millis(500));
        assert_eq!(limits.runner_max_attempts, 2);
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
                    max_key_bytes: Some(MAX_KEY_BYTES + 1),
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
                    max_value_bytes: Some(MAX_VALUE_BYTES + 1),
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
                    max_transaction_operations: Some(MAX_TRANSACTION_OPERATIONS + 1),
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
                    max_transaction_bytes: Some(MAX_TRANSACTION_BYTES + 1),
                    ..Default::default()
                },
            ),
            (
                "transaction_deadline_ms",
                StateStoreLimitOverrides {
                    transaction_deadline_ms: Some(0),
                    ..Default::default()
                },
            ),
            (
                "transaction_deadline_ms",
                StateStoreLimitOverrides {
                    transaction_deadline_ms: Some(
                        DEFAULT_TRANSACTION_DEADLINE.as_millis() as u64 + 1,
                    ),
                    ..Default::default()
                },
            ),
            (
                "runner_max_attempts",
                StateStoreLimitOverrides {
                    runner_max_attempts: Some(0),
                    ..Default::default()
                },
            ),
            (
                "runner_max_attempts",
                StateStoreLimitOverrides {
                    runner_max_attempts: Some(MAX_RUNNER_ATTEMPTS + 1),
                    ..Default::default()
                },
            ),
        ];

        for (field, overrides) in invalid {
            let error = resolve_state_store_limits(&overrides, MAX_KEY_BYTES)
                .expect_err("zero or relaxed limits must fail closed");
            let message = error.to_string();
            assert!(message.contains("InvalidStateStoreConfig"), "{message}");
            assert!(message.contains(field), "{message}");
        }
    }
}
