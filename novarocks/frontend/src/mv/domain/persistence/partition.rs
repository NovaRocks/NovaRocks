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

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::mv::domain::persistence::schema::MvPartitionContract;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MvPartitionRefreshStatus {
    Fresh,
    Refreshing,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredMvPartitionState {
    pub mv_id: i64,
    pub partition_key: String,
    pub status: MvPartitionRefreshStatus,
    pub last_refresh_ms: Option<i64>,
    pub base_snapshots: BTreeMap<String, i64>,
    pub target_snapshot_id: Option<i64>,
    pub last_refresh_id: Option<i64>,
    pub failure_message: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplaceMvPartitionStatesRequest {
    pub mv_id: i64,
    pub partition_keys: BTreeSet<String>,
    pub last_refresh_ms: i64,
    pub base_snapshots: BTreeMap<String, i64>,
    pub target_snapshot_id: Option<i64>,
    pub last_refresh_id: i64,
    pub max_entries: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordFailedMvPartitionStatesRequest {
    pub mv_id: i64,
    pub partition_keys: BTreeSet<String>,
    pub failure_message: String,
    pub last_refresh_ms: i64,
    pub base_snapshots: BTreeMap<String, i64>,
    pub target_snapshot_id: Option<i64>,
    pub last_refresh_id: i64,
    pub max_entries: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateMvPartitionContractRequest {
    pub mv_id: i64,
    pub partition_spec: MvPartitionContract,
}
