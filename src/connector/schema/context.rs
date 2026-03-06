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
use crate::plan_nodes;

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) struct SchemaScanContext {
    pub(crate) table_name: String,
    pub(crate) db: Option<String>,
    pub(crate) table: Option<String>,
    pub(crate) table_id: Option<i64>,
    pub(crate) partition_id: Option<i64>,
    pub(crate) tablet_id: Option<i64>,
    pub(crate) txn_id: Option<i64>,
    pub(crate) type_: Option<String>,
    pub(crate) state: Option<String>,
    pub(crate) limit: Option<i64>,
    pub(crate) log_start_ts: Option<i64>,
    pub(crate) log_end_ts: Option<i64>,
    pub(crate) log_level: Option<String>,
    pub(crate) log_pattern: Option<String>,
    pub(crate) log_limit: Option<i64>,
}

impl SchemaScanContext {
    pub(crate) fn from_thrift(node: &plan_nodes::TSchemaScanNode) -> Self {
        Self {
            table_name: node.table_name.trim().to_ascii_lowercase(),
            db: normalize_optional_string(node.db.as_ref()),
            table: normalize_optional_string(node.table.as_ref()),
            table_id: node.table_id.filter(|value| *value > 0),
            partition_id: node.partition_id.filter(|value| *value > 0),
            tablet_id: node.tablet_id.filter(|value| *value > 0),
            txn_id: node.txn_id.filter(|value| *value > 0),
            type_: normalize_optional_string(node.type_.as_ref())
                .map(|value| value.to_ascii_uppercase()),
            state: normalize_optional_string(node.state.as_ref())
                .map(|value| value.to_ascii_uppercase()),
            limit: node.limit.filter(|value| *value >= 0),
            log_start_ts: node.log_start_ts.filter(|value| *value > 0),
            log_end_ts: node.log_end_ts.filter(|value| *value > 0),
            log_level: normalize_optional_string(node.log_level.as_ref())
                .map(|value| value.to_ascii_uppercase()),
            log_pattern: normalize_optional_string(node.log_pattern.as_ref()),
            log_limit: node.log_limit.filter(|value| *value > 0),
        }
    }

    pub(crate) fn limit_as_usize(&self) -> Option<usize> {
        self.limit
            .and_then(|value| usize::try_from(value).ok())
            .filter(|value| *value > 0)
    }
}

fn normalize_optional_string(value: Option<&String>) -> Option<String> {
    value
        .map(|raw| raw.trim().to_string())
        .filter(|raw| !raw.is_empty())
}
