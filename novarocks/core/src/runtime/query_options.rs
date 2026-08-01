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

use std::time::Duration;

use crate::exec::spill::SpillConfig;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueryOptions {
    pub(crate) batch_size: Option<i32>,
    pub(crate) query_timeout: Option<i32>,
    pub(crate) query_delivery_timeout: Option<i32>,
    pub(crate) enable_profile: bool,
    pub(crate) runtime_profile_report_interval: Option<i64>,
    pub(crate) pipeline_dop: Option<i32>,
    pub(crate) exec_mem_limit: Option<i64>,
    pub(crate) connector_io_tasks_per_scan_operator: Option<i32>,
    pub(crate) orc_use_column_names: bool,
    pub(crate) enable_file_metacache: bool,
    pub(crate) enable_file_pagecache: bool,
    pub(crate) enable_parquet_reader_page_index: bool,
    pub(crate) runtime_filter_scan_wait_time_ms: Option<i64>,
    pub(crate) runtime_filter_wait_timeout_ms: Option<i32>,
    pub(crate) allow_throw_exception: bool,
    pub(crate) group_concat_max_len: Option<i64>,
    pub(crate) enable_join_runtime_bitset_filter: Option<bool>,
    pub(crate) global_runtime_filter_build_max_size: Option<i64>,
    pub(crate) cache: QueryCacheOptions,
    pub(crate) spill: Option<SpillConfig>,
}

/// Protocol-neutral query execution options captured at an ingress boundary.
///
/// The execution kernel keeps its `QueryOptions` representation private; an
/// adapter constructs it through this value object instead of mutating runtime
/// state field-by-field.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueryOptionsParts {
    pub batch_size: Option<i32>,
    pub query_timeout: Option<i32>,
    pub query_delivery_timeout: Option<i32>,
    pub enable_profile: bool,
    pub runtime_profile_report_interval: Option<i64>,
    pub pipeline_dop: Option<i32>,
    pub exec_mem_limit: Option<i64>,
    pub connector_io_tasks_per_scan_operator: Option<i32>,
    pub orc_use_column_names: bool,
    pub enable_file_metacache: bool,
    pub enable_file_pagecache: bool,
    pub enable_parquet_reader_page_index: bool,
    pub runtime_filter_scan_wait_time_ms: Option<i64>,
    pub runtime_filter_wait_timeout_ms: Option<i32>,
    pub allow_throw_exception: bool,
    pub group_concat_max_len: Option<i64>,
    pub enable_join_runtime_bitset_filter: Option<bool>,
    pub global_runtime_filter_build_max_size: Option<i64>,
    pub cache: QueryCacheOptions,
    pub spill: Option<SpillConfig>,
}

impl QueryOptions {
    /// Applies the per-statement optimizer hints that affect runtime
    /// expression semantics. These hints must be attached before a query is
    /// prepared so that every distributed fragment receives the same options.
    pub fn apply_sql_hints(&mut self, sql: &str) {
        self.allow_throw_exception =
            crate::sql::parser::set_var_hint::extract_allow_throw_exception(sql);
    }

    pub fn from_parts(parts: QueryOptionsParts) -> Self {
        Self {
            batch_size: parts.batch_size,
            query_timeout: parts.query_timeout,
            query_delivery_timeout: parts.query_delivery_timeout,
            enable_profile: parts.enable_profile,
            runtime_profile_report_interval: parts.runtime_profile_report_interval,
            pipeline_dop: parts.pipeline_dop,
            exec_mem_limit: parts.exec_mem_limit,
            connector_io_tasks_per_scan_operator: parts.connector_io_tasks_per_scan_operator,
            orc_use_column_names: parts.orc_use_column_names,
            enable_file_metacache: parts.enable_file_metacache,
            enable_file_pagecache: parts.enable_file_pagecache,
            enable_parquet_reader_page_index: parts.enable_parquet_reader_page_index,
            runtime_filter_scan_wait_time_ms: parts.runtime_filter_scan_wait_time_ms,
            runtime_filter_wait_timeout_ms: parts.runtime_filter_wait_timeout_ms,
            allow_throw_exception: parts.allow_throw_exception,
            group_concat_max_len: parts.group_concat_max_len,
            enable_join_runtime_bitset_filter: parts.enable_join_runtime_bitset_filter,
            global_runtime_filter_build_max_size: parts.global_runtime_filter_build_max_size,
            cache: parts.cache,
            spill: parts.spill,
        }
    }

    pub const fn batch_size(&self) -> Option<i32> {
        self.batch_size
    }

    pub const fn pipeline_dop(&self) -> Option<i32> {
        self.pipeline_dop
    }

    pub const fn query_timeout(&self) -> Option<i32> {
        self.query_timeout
    }

    pub const fn exec_mem_limit(&self) -> Option<i64> {
        self.exec_mem_limit
    }

    pub const fn connector_io_tasks_per_scan_operator(&self) -> Option<i32> {
        self.connector_io_tasks_per_scan_operator
    }

    pub const fn orc_use_column_names(&self) -> bool {
        self.orc_use_column_names
    }

    pub const fn enable_file_metacache(&self) -> bool {
        self.enable_file_metacache
    }

    pub const fn enable_file_pagecache(&self) -> bool {
        self.enable_file_pagecache
    }

    pub const fn allow_throw_exception(&self) -> bool {
        self.allow_throw_exception
    }

    pub const fn group_concat_max_len(&self) -> Option<i64> {
        self.group_concat_max_len
    }

    pub const fn runtime_filter_scan_wait_time_ms(&self) -> Option<i64> {
        self.runtime_filter_scan_wait_time_ms
    }

    pub const fn runtime_filter_wait_timeout_ms(&self) -> Option<i32> {
        self.runtime_filter_wait_timeout_ms
    }

    pub const fn enable_join_runtime_bitset_filter(&self) -> Option<bool> {
        self.enable_join_runtime_bitset_filter
    }

    pub const fn enable_parquet_reader_page_index(&self) -> bool {
        self.enable_parquet_reader_page_index
    }

    pub const fn cache(&self) -> &QueryCacheOptions {
        &self.cache
    }

    pub fn spill(&self) -> Option<&SpillConfig> {
        self.spill.as_ref()
    }

    pub const fn enable_profile(&self) -> bool {
        self.enable_profile
    }

    pub const fn runtime_profile_report_interval(&self) -> Option<i64> {
        self.runtime_profile_report_interval
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueryCacheOptions {
    pub enable_scan_datacache: bool,
    pub enable_populate_datacache: bool,
    pub enable_datacache_async_populate_mode: bool,
    pub enable_datacache_io_adaptor: bool,
    pub enable_cache_select: bool,
    pub datacache_evict_probability: Option<i32>,
    pub datacache_priority: Option<i32>,
    pub datacache_ttl_seconds: Option<i64>,
    pub datacache_sharing_work_period: Option<i64>,
}

pub fn query_expire_durations(query_opts: Option<&QueryOptions>) -> (Duration, Duration) {
    let default_timeout = 300i32;
    let query_timeout = query_opts
        .and_then(|o| o.query_timeout)
        .unwrap_or(default_timeout)
        .max(1);
    let delivery_timeout = query_opts
        .and_then(|o| o.query_delivery_timeout)
        .map(|v| v.max(1).min(query_timeout))
        .unwrap_or(query_timeout);
    (
        Duration::from_secs(delivery_timeout as u64),
        Duration::from_secs(query_timeout as u64),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_expire_durations_use_delivery_timeout_cap() {
        let native = QueryOptions {
            query_timeout: Some(60),
            query_delivery_timeout: Some(120),
            ..Default::default()
        };
        let (delivery, query) = query_expire_durations(Some(&native));
        assert_eq!(query.as_secs(), 60);
        assert_eq!(delivery.as_secs(), 60);
    }
}
