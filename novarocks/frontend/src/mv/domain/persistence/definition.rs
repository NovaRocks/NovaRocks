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

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::mv::domain::persistence::schema::{MvPartitionContract, MvSchemaContract};

pub(crate) const MV_DEFINITION_SUBJECT: &str = "mv.definition";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredMvDefinition {
    pub mv_id: i64,
    pub select_sql: String,
    pub base_table_refs: Vec<String>,
    pub primary_key_columns: Vec<String>,
    pub storage_engine: String,
    pub target_catalog: Option<String>,
    pub target_namespace: Option<String>,
    pub target_table: Option<String>,
    #[serde(default)]
    pub schema_contract: Option<MvSchemaContract>,
    #[serde(default)]
    pub partition_spec: Option<MvPartitionContract>,
    #[serde(default)]
    pub partition_state_complete: bool,
    pub last_refresh_ms: Option<i64>,
    pub last_refresh_rows: Option<i64>,
    pub last_refresh_snapshots: BTreeMap<String, i64>,
    pub last_refresh_table_uuids: BTreeMap<String, String>,
    pub last_refreshed_iceberg_snapshot_id: Option<i64>,
    pub refresh_in_progress: bool,
    #[serde(default)]
    pub active_refresh_id: Option<i64>,
    pub refresh_target_snapshots: BTreeMap<String, i64>,
    #[serde(default)]
    pub refresh_policy: StoredMvRefreshPolicy,
    #[serde(default)]
    pub refresh_paused: bool,
    #[serde(default)]
    pub refresh_interval_ms: Option<i64>,
    #[serde(default)]
    pub max_staleness_ms: Option<i64>,
    #[serde(default)]
    pub last_scheduler_error: Option<String>,
    #[serde(default)]
    pub next_refresh_after_ms: Option<i64>,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateMvDefinitionRequest {
    pub select_sql: String,
    pub base_table_refs: Vec<String>,
    pub primary_key_columns: Vec<String>,
    pub storage_engine: String,
    pub target_catalog: Option<String>,
    pub target_namespace: Option<String>,
    pub target_table: Option<String>,
    pub schema_contract: Option<MvSchemaContract>,
    pub partition_spec: Option<MvPartitionContract>,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateMvRefreshMetadataRequest {
    pub mv_id: i64,
    pub refresh_policy: StoredMvRefreshPolicy,
    pub refresh_paused: bool,
    pub refresh_interval_ms: Option<i64>,
    pub max_staleness_ms: Option<i64>,
    pub last_scheduler_error: Option<String>,
    pub next_refresh_after_ms: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredMvDefinitionAvro {
    mv_id: i64,
    select_sql: String,
    base_table_refs: Vec<String>,
    primary_key_columns: Vec<String>,
    storage_engine: String,
    target_catalog: Option<String>,
    target_namespace: Option<String>,
    target_table: Option<String>,
    schema_contract: Option<String>,
    partition_spec: Option<String>,
    #[serde(default)]
    partition_state_complete: bool,
    last_refresh_ms: Option<i64>,
    last_refresh_rows: Option<i64>,
    last_refresh_snapshots: BTreeMap<String, i64>,
    last_refresh_table_uuids: BTreeMap<String, String>,
    last_refreshed_iceberg_snapshot_id: Option<i64>,
    refresh_in_progress: bool,
    active_refresh_id: Option<i64>,
    refresh_target_snapshots: BTreeMap<String, i64>,
    refresh_policy: StoredMvRefreshPolicy,
    refresh_paused: bool,
    refresh_interval_ms: Option<i64>,
    max_staleness_ms: Option<i64>,
    last_scheduler_error: Option<String>,
    next_refresh_after_ms: Option<i64>,
    created_at_ms: i64,
}

#[derive(Debug)]
pub(crate) enum MvDefinitionCodecError {
    EncodeSchemaContract(serde_json::Error),
    EncodePartitionContract(serde_json::Error),
    DecodeSchemaContract(serde_json::Error),
    DecodePartitionContract(serde_json::Error),
}

impl fmt::Display for MvDefinitionCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EncodeSchemaContract(err) => {
                write!(
                    formatter,
                    "failed to encode MV schema contract as JSON: {err}"
                )
            }
            Self::EncodePartitionContract(err) => write!(
                formatter,
                "failed to encode MV partition contract as JSON: {err}"
            ),
            Self::DecodeSchemaContract(err) => {
                write!(formatter, "failed to decode MV schema contract JSON: {err}")
            }
            Self::DecodePartitionContract(err) => write!(
                formatter,
                "failed to decode MV partition contract JSON: {err}"
            ),
        }
    }
}

impl std::error::Error for MvDefinitionCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::EncodeSchemaContract(err)
            | Self::EncodePartitionContract(err)
            | Self::DecodeSchemaContract(err)
            | Self::DecodePartitionContract(err) => Some(err),
        }
    }
}

impl TryFrom<&StoredMvDefinition> for StoredMvDefinitionAvro {
    type Error = MvDefinitionCodecError;

    fn try_from(value: &StoredMvDefinition) -> Result<Self, Self::Error> {
        Ok(Self {
            mv_id: value.mv_id,
            select_sql: value.select_sql.clone(),
            base_table_refs: value.base_table_refs.clone(),
            primary_key_columns: value.primary_key_columns.clone(),
            storage_engine: value.storage_engine.clone(),
            target_catalog: value.target_catalog.clone(),
            target_namespace: value.target_namespace.clone(),
            target_table: value.target_table.clone(),
            schema_contract: value
                .schema_contract
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(MvDefinitionCodecError::EncodeSchemaContract)?,
            partition_spec: value
                .partition_spec
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(MvDefinitionCodecError::EncodePartitionContract)?,
            partition_state_complete: value.partition_state_complete,
            last_refresh_ms: value.last_refresh_ms,
            last_refresh_rows: value.last_refresh_rows,
            last_refresh_snapshots: value.last_refresh_snapshots.clone(),
            last_refresh_table_uuids: value.last_refresh_table_uuids.clone(),
            last_refreshed_iceberg_snapshot_id: value.last_refreshed_iceberg_snapshot_id,
            refresh_in_progress: value.refresh_in_progress,
            active_refresh_id: value.active_refresh_id,
            refresh_target_snapshots: value.refresh_target_snapshots.clone(),
            refresh_policy: value.refresh_policy.clone(),
            refresh_paused: value.refresh_paused,
            refresh_interval_ms: value.refresh_interval_ms,
            max_staleness_ms: value.max_staleness_ms,
            last_scheduler_error: value.last_scheduler_error.clone(),
            next_refresh_after_ms: value.next_refresh_after_ms,
            created_at_ms: value.created_at_ms,
        })
    }
}

impl TryFrom<StoredMvDefinitionAvro> for StoredMvDefinition {
    type Error = MvDefinitionCodecError;

    fn try_from(value: StoredMvDefinitionAvro) -> Result<Self, Self::Error> {
        Ok(Self {
            mv_id: value.mv_id,
            select_sql: value.select_sql,
            base_table_refs: value.base_table_refs,
            primary_key_columns: value.primary_key_columns,
            storage_engine: value.storage_engine,
            target_catalog: value.target_catalog,
            target_namespace: value.target_namespace,
            target_table: value.target_table,
            schema_contract: value
                .schema_contract
                .as_deref()
                .map(serde_json::from_str::<MvSchemaContract>)
                .transpose()
                .map_err(MvDefinitionCodecError::DecodeSchemaContract)?,
            partition_spec: value
                .partition_spec
                .as_deref()
                .map(serde_json::from_str::<MvPartitionContract>)
                .transpose()
                .map_err(MvDefinitionCodecError::DecodePartitionContract)?,
            partition_state_complete: value.partition_state_complete,
            last_refresh_ms: value.last_refresh_ms,
            last_refresh_rows: value.last_refresh_rows,
            last_refresh_snapshots: value.last_refresh_snapshots,
            last_refresh_table_uuids: value.last_refresh_table_uuids,
            last_refreshed_iceberg_snapshot_id: value.last_refreshed_iceberg_snapshot_id,
            refresh_in_progress: value.refresh_in_progress,
            active_refresh_id: value.active_refresh_id,
            refresh_target_snapshots: value.refresh_target_snapshots,
            refresh_policy: value.refresh_policy,
            refresh_paused: value.refresh_paused,
            refresh_interval_ms: value.refresh_interval_ms,
            max_staleness_ms: value.max_staleness_ms,
            last_scheduler_error: value.last_scheduler_error,
            next_refresh_after_ms: value.next_refresh_after_ms,
            created_at_ms: value.created_at_ms,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StoredMvRefreshPolicy {
    #[default]
    Manual,
    AsyncOnChange,
    AsyncInterval,
}

impl StoredMvRefreshPolicy {
    pub fn as_sql_str(&self) -> &'static str {
        match self {
            Self::Manual => "DEFERRED_MANUAL",
            Self::AsyncOnChange => "ASYNC_ON_CHANGE",
            Self::AsyncInterval => "ASYNC_INTERVAL",
        }
    }

    pub(crate) fn accepts_interval(&self) -> bool {
        matches!(self, Self::AsyncInterval)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn avro_with_contract_json(
        schema_contract: Option<String>,
        partition_spec: Option<String>,
    ) -> StoredMvDefinitionAvro {
        StoredMvDefinitionAvro {
            mv_id: 1,
            select_sql: "SELECT 1".to_string(),
            base_table_refs: Vec::new(),
            primary_key_columns: Vec::new(),
            storage_engine: "iceberg".to_string(),
            target_catalog: None,
            target_namespace: None,
            target_table: None,
            schema_contract,
            partition_spec,
            partition_state_complete: false,
            last_refresh_ms: None,
            last_refresh_rows: None,
            last_refresh_snapshots: BTreeMap::new(),
            last_refresh_table_uuids: BTreeMap::new(),
            last_refreshed_iceberg_snapshot_id: None,
            refresh_in_progress: false,
            active_refresh_id: None,
            refresh_target_snapshots: BTreeMap::new(),
            refresh_policy: StoredMvRefreshPolicy::Manual,
            refresh_paused: false,
            refresh_interval_ms: None,
            max_staleness_ms: None,
            last_scheduler_error: None,
            next_refresh_after_ms: None,
            created_at_ms: 1,
        }
    }

    #[test]
    fn absent_nested_contracts_remain_absent() {
        let definition = StoredMvDefinition::try_from(avro_with_contract_json(None, None))
            .expect("absent contracts should decode");
        assert_eq!(definition.schema_contract, None);
        assert_eq!(definition.partition_spec, None);
    }

    #[test]
    fn malformed_nested_contracts_fail_with_stable_context() {
        let schema_err = StoredMvDefinition::try_from(avro_with_contract_json(
            Some("not-json".to_string()),
            None,
        ))
        .expect_err("malformed schema contract should fail");
        assert!(
            schema_err
                .to_string()
                .contains("failed to decode MV schema contract JSON")
        );

        let partition_err = StoredMvDefinition::try_from(avro_with_contract_json(
            None,
            Some("not-json".to_string()),
        ))
        .expect_err("malformed partition contract should fail");
        assert!(
            partition_err
                .to_string()
                .contains("failed to decode MV partition contract JSON")
        );
    }
}
