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

//! Product-owned refresh configuration transitions.

use crate::persistence::definition::MvDesiredRefreshPolicy;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MvRefreshDesiredConfiguration {
    pub policy: MvDesiredRefreshPolicy,
    pub paused: bool,
    pub interval_ms: Option<i64>,
    pub max_staleness_ms: Option<i64>,
}

impl MvRefreshDesiredConfiguration {
    pub fn new(
        policy: MvDesiredRefreshPolicy,
        paused: bool,
        interval_ms: Option<i64>,
        max_staleness_ms: Option<i64>,
    ) -> Result<Self, String> {
        let value = Self {
            policy,
            paused,
            interval_ms,
            max_staleness_ms,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), String> {
        match self.policy {
            MvDesiredRefreshPolicy::Manual | MvDesiredRefreshPolicy::AsyncOnChange
                if self.interval_ms.is_some() =>
            {
                return Err(format!(
                    "MV refresh policy {} must not carry interval_ms",
                    self.policy.as_sql_str()
                ));
            }
            MvDesiredRefreshPolicy::AsyncInterval
                if self.interval_ms.is_none_or(|value| value <= 0) =>
            {
                return Err(
                    "MV refresh policy ASYNC_INTERVAL requires a positive interval_ms".to_string(),
                );
            }
            _ => {}
        }
        if self.max_staleness_ms.is_some_and(|value| value <= 0) {
            return Err("MV refresh max_staleness_ms must be positive when set".to_string());
        }
        Ok(())
    }

    /// Apply a SQL-lowered refresh-policy replacement while retaining the
    /// product-owned pause and staleness facts.
    pub fn with_policy(
        &self,
        policy: MvDesiredRefreshPolicy,
        interval_ms: Option<i64>,
    ) -> Result<Self, String> {
        Self::new(policy, self.paused, interval_ms, self.max_staleness_ms)
    }

    /// Apply the product-owned pause transition without letting an adapter
    /// rebuild or default any other durable refresh fact.
    pub fn with_paused(&self, paused: bool) -> Result<Self, String> {
        Self::new(
            self.policy.clone(),
            paused,
            self.interval_ms,
            self.max_staleness_ms,
        )
    }
}
