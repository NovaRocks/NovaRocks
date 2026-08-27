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

//! Test-only input compatibility for the MySQL provider harness.

use novarocks_spi::state_store::{
    DEFAULT_TRANSACTION_DEADLINE, MAX_PAGE_SIZE, MAX_RUNNER_ATTEMPTS, MAX_TRANSACTION_BYTES,
    MAX_TRANSACTION_OPERATIONS, MAX_VALUE_BYTES, StateStoreError, StateStoreErrorKind,
    StateStoreLimits,
};

use crate::{MYSQL_MAX_KEY_BYTES, MysqlStateStoreOpenConfig};

#[doc(hidden)]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MysqlTestLimitOverrides {
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
pub enum MysqlTestProviderConfig {
    Mysql { database: String },
}

#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MysqlTestStoreConfig {
    pub cluster_id: String,
    pub limits: MysqlTestLimitOverrides,
    pub provider: MysqlTestProviderConfig,
}

impl MysqlTestStoreConfig {
    pub(crate) fn into_mysql_open(self) -> Result<MysqlStateStoreOpenConfig, StateStoreError> {
        let MysqlTestProviderConfig::Mysql { database } = self.provider;
        Ok(MysqlStateStoreOpenConfig {
            cluster_id: self.cluster_id,
            database,
            limits: resolve_limits(&self.limits)?,
        })
    }
}

fn resolve_limits(
    overrides: &MysqlTestLimitOverrides,
) -> Result<StateStoreLimits, StateStoreError> {
    Ok(StateStoreLimits {
        max_key_bytes: tightened_usize(
            "max_key_bytes",
            overrides.max_key_bytes,
            MYSQL_MAX_KEY_BYTES,
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
        transaction_deadline: std::time::Duration::from_millis(tightened_u64(
            "transaction_deadline_ms",
            overrides.transaction_deadline_ms,
            DEFAULT_TRANSACTION_DEADLINE.as_millis() as u64,
        )?),
        runner_max_attempts: tightened_usize(
            "runner_max_attempts",
            overrides.runner_max_attempts,
            MAX_RUNNER_ATTEMPTS,
        )?,
    })
}

fn tightened_usize(
    name: &str,
    value: Option<usize>,
    maximum: usize,
) -> Result<usize, StateStoreError> {
    let value = value.unwrap_or(maximum);
    if value == 0 || value > maximum {
        let _ = (name, maximum);
        return Err(invalid_limit());
    }
    Ok(value)
}

fn tightened_u64(name: &str, value: Option<u64>, maximum: u64) -> Result<u64, StateStoreError> {
    let value = value.unwrap_or(maximum);
    if value == 0 || value > maximum {
        let _ = (name, maximum);
        return Err(invalid_limit());
    }
    Ok(value)
}

fn invalid_limit() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::InvalidConfiguration,
        "MySQL test state store limits are invalid",
    )
}
