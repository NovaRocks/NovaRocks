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

use novarocks_spi::connector::ConnectorTableObjectId;
use serde::{Deserialize, Serialize};

use crate::persistence::schema::{MvPartitionContract, MvSchemaContract};
use novarocks_query_application::persisted_query_definition::PersistedQueryDefinition;

pub(crate) const MV_ACCELERATOR_PROJECTION_SUBJECT: &str = "mv.accelerator_projection";

/// Exact lake revision from which one accelerator projection was derived.
///
/// The object identity is opaque outside the provider. It is deliberately
/// persisted with the descriptor digest and the current target snapshot so a
/// logical-name ABA cannot authorize a stale replace or deletion.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MvAcceleratorSourceRevision {
    pub target_object_id: ConnectorTableObjectId,
    pub descriptor_content_hash: String,
    pub current_target_snapshot_id: Option<i64>,
}

/// Lake-derived materialized-view accelerator root.
///
/// This record contains canonical desired facts and aggregate published facts
/// only. Active attempts, scheduler state, partition freshness and recovery
/// state are process runtime and must never be added to this payload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredMvDefinition {
    pub mv_id: i64,
    pub query_definition: PersistedQueryDefinition,
    pub base_table_refs: Vec<String>,
    pub primary_key_columns: Vec<String>,
    pub storage_engine: String,
    pub target_catalog: Option<String>,
    pub target_namespace: Option<String>,
    pub target_table: Option<String>,
    pub schema_contract: Option<MvSchemaContract>,
    pub partition_spec: Option<MvPartitionContract>,
    pub last_refresh_ms: Option<i64>,
    pub last_refresh_rows: Option<i64>,
    pub last_refresh_snapshots: BTreeMap<String, i64>,
    pub last_refresh_table_object_ids: BTreeMap<String, ConnectorTableObjectId>,
    pub last_refreshed_iceberg_snapshot_id: Option<i64>,
    pub refresh_policy: MvDesiredRefreshPolicy,
    pub refresh_paused: bool,
    pub refresh_interval_ms: Option<i64>,
    pub max_staleness_ms: Option<i64>,
    pub created_at_ms: i64,
    pub source_revision: MvAcceleratorSourceRevision,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateMvDefinitionRequest {
    pub query_definition: PersistedQueryDefinition,
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

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MvDesiredRefreshPolicy {
    #[default]
    Manual,
    AsyncOnChange,
    AsyncInterval,
}

impl MvDesiredRefreshPolicy {
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
