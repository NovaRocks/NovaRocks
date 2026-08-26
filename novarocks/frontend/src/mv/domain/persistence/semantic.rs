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

//! Canonical MV desired semantics shared by lake descriptor writers and
//! rebuilders. Runtime identities and scheduler bookkeeping intentionally do
//! not enter this value.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::common::persisted_query_definition::PersistedQueryDefinition;
use crate::mv::domain::persistence::definition::StoredMvRefreshPolicy;
use crate::mv::domain::persistence::descriptor::DescriptorDependency;
use crate::mv::domain::persistence::schema::MvSchemaContract;
use novarocks_types::naming::normalize_identifier;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MvRefreshDesiredConfiguration {
    pub policy: StoredMvRefreshPolicy,
    pub paused: bool,
    pub interval_ms: Option<i64>,
    pub max_staleness_ms: Option<i64>,
}

impl MvRefreshDesiredConfiguration {
    pub fn new(
        policy: StoredMvRefreshPolicy,
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
            StoredMvRefreshPolicy::Manual | StoredMvRefreshPolicy::AsyncOnChange
                if self.interval_ms.is_some() =>
            {
                return Err(format!(
                    "MV refresh policy {} must not carry interval_ms",
                    self.policy.as_sql_str()
                ));
            }
            StoredMvRefreshPolicy::AsyncInterval
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvDesiredSemantics {
    pub package_id: String,
    pub query_definition: PersistedQueryDefinition,
    pub visible_columns: Vec<String>,
    pub hidden_columns: Vec<String>,
    pub base_dependencies: Vec<DescriptorDependency>,
    pub primary_key_columns: Vec<String>,
    pub schema_contract: MvSchemaContract,
    pub refresh: MvRefreshDesiredConfiguration,
    pub created_at_ms: i64,
}

impl MvDesiredSemantics {
    #[expect(
        clippy::too_many_arguments,
        reason = "The constructor makes every lake-authoritative MV semantic field explicit."
    )]
    pub fn new(
        package_id: String,
        query_definition: PersistedQueryDefinition,
        visible_columns: Vec<String>,
        hidden_columns: Vec<String>,
        base_dependencies: Vec<DescriptorDependency>,
        primary_key_columns: Vec<String>,
        schema_contract: MvSchemaContract,
        refresh: MvRefreshDesiredConfiguration,
        created_at_ms: i64,
    ) -> Result<Self, String> {
        if package_id.trim().is_empty() {
            return Err("MV descriptor package_id must not be empty".to_string());
        }
        query_definition
            .validate()
            .map_err(|error| format!("invalid MV descriptor query definition: {error}"))?;
        if query_definition.raw_query_source.len()
            > super::descriptor::MV_DESCRIPTOR_RAW_QUERY_SOURCE_MAX_BYTES
        {
            return Err(format!(
                "MV descriptor raw query source exceeds 64KiB cap of {} bytes",
                super::descriptor::MV_DESCRIPTOR_RAW_QUERY_SOURCE_MAX_BYTES
            ));
        }
        if visible_columns.is_empty() {
            return Err("MV descriptor must contain at least one visible column".to_string());
        }
        validate_column_list("visible_columns", &visible_columns)?;
        validate_column_list("hidden_columns", &hidden_columns)?;
        validate_dependencies(&base_dependencies)?;
        let primary_key_columns = normalize_primary_key_columns(primary_key_columns)?;
        refresh.validate()?;
        if created_at_ms < 0 {
            return Err("MV descriptor created_at_ms must not be negative".to_string());
        }
        Ok(Self {
            package_id,
            query_definition,
            visible_columns,
            hidden_columns,
            base_dependencies,
            primary_key_columns,
            schema_contract,
            refresh,
            created_at_ms,
        })
    }
}

fn validate_column_list(field: &str, columns: &[String]) -> Result<(), String> {
    let mut seen = BTreeSet::new();
    for column in columns {
        let canonical = normalize_identifier(column).map_err(|error| {
            format!("MV descriptor {field} has invalid identifier `{column}`: {error}")
        })?;
        if canonical != *column {
            return Err(format!(
                "MV descriptor {field} identifier `{column}` is not canonical `{canonical}`"
            ));
        }
        if !seen.insert(canonical) {
            return Err(format!(
                "MV descriptor {field} has duplicate identifier `{column}`"
            ));
        }
    }
    Ok(())
}

fn normalize_primary_key_columns(columns: Vec<String>) -> Result<Vec<String>, String> {
    let mut seen = BTreeSet::new();
    columns
        .into_iter()
        .map(|column| {
            let canonical = normalize_identifier(&column).map_err(|error| {
                format!(
                    "MV descriptor primary_key_columns has invalid identifier `{column}`: {error}"
                )
            })?;
            if !seen.insert(canonical.clone()) {
                return Err(format!(
                    "MV descriptor primary_key_columns has duplicate identifier `{canonical}`"
                ));
            }
            Ok(canonical)
        })
        .collect()
}

fn validate_dependencies(dependencies: &[DescriptorDependency]) -> Result<(), String> {
    if dependencies.is_empty() {
        return Err("MV descriptor must contain at least one base dependency".to_string());
    }
    let mut seen = BTreeSet::new();
    for dependency in dependencies {
        for (name, value) in [
            ("catalog", dependency.catalog.as_str()),
            ("namespace", dependency.namespace.as_str()),
            ("name", dependency.name.as_str()),
            ("object_type", dependency.object_type.as_str()),
            ("storage_engine", dependency.storage_engine.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(format!("MV descriptor dependency {name} must not be empty"));
            }
        }
        let identity = format!(
            "{}.{}.{}.{}.{}",
            dependency.catalog,
            dependency.namespace,
            dependency.name,
            dependency.object_type,
            dependency.storage_engine
        );
        if !seen.insert(identity) {
            return Err("MV descriptor has duplicate base dependency".to_string());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refresh_configuration_rejects_interval_for_manual() {
        let error =
            MvRefreshDesiredConfiguration::new(StoredMvRefreshPolicy::Manual, false, Some(1), None)
                .unwrap_err();
        assert!(error.contains("must not carry interval_ms"));
    }

    #[test]
    fn refresh_configuration_requires_positive_interval_and_staleness() {
        let error = MvRefreshDesiredConfiguration::new(
            StoredMvRefreshPolicy::AsyncInterval,
            false,
            Some(0),
            None,
        )
        .unwrap_err();
        assert!(error.contains("positive interval_ms"));

        let error = MvRefreshDesiredConfiguration::new(
            StoredMvRefreshPolicy::AsyncOnChange,
            true,
            None,
            Some(0),
        )
        .unwrap_err();
        assert!(error.contains("max_staleness_ms must be positive"));
    }

    #[test]
    fn primary_key_columns_are_normalized_and_ordered() {
        let columns =
            normalize_primary_key_columns(vec!["`Second`".to_string(), "first".to_string()])
                .expect("valid primary key");
        assert_eq!(columns, vec!["second", "first"]);
    }
}
