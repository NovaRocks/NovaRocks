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

/// Per-range cache facts frozen before local scan execution.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ExternalDataCacheRangeOptions {
    pub modification_time: Option<i64>,
    pub enable_populate_datacache: Option<bool>,
    pub datacache_priority: Option<i32>,
    pub candidate_node: Option<String>,
}

/// Frozen cache policy consumed by the local execution kernel.
///
/// Host-specific cache-manager and filesystem conversions remain outside this
/// crate; execution only carries the resolved request-level facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionCacheOptions {
    pub enable_scan_datacache: bool,
    pub enable_populate_datacache: bool,
    pub enable_datacache_async_populate_mode: bool,
    pub enable_datacache_io_adaptor: bool,
    pub enable_cache_select: bool,
    pub datacache_evict_probability: i32,
    pub datacache_priority: i32,
    pub datacache_ttl_seconds: i64,
    pub datacache_sharing_work_period: Option<i64>,
}

impl ExecutionCacheOptions {
    pub fn from_query_options(
        options: Option<&crate::runtime::query_options::QueryOptions>,
    ) -> Result<Self, String> {
        let cache = options.map(|options| &options.cache);
        let probability = cache
            .and_then(|cache| cache.datacache_evict_probability)
            .unwrap_or(100);
        if !(0..=100).contains(&probability) {
            return Err(format!(
                "invalid query option: datacache_evict_probability must be in [0, 100], got {probability}"
            ));
        }
        let priority = cache
            .and_then(|cache| cache.datacache_priority)
            .unwrap_or(0);
        if !(-1..=127).contains(&priority) {
            return Err(format!(
                "invalid query option: datacache_priority must be in [-1, 127], got {priority}"
            ));
        }
        let ttl_seconds = cache
            .and_then(|cache| cache.datacache_ttl_seconds)
            .unwrap_or(0);
        if ttl_seconds < 0 {
            return Err(format!(
                "invalid query option: datacache_ttl_seconds must be non-negative, got {ttl_seconds}"
            ));
        }
        Ok(Self {
            enable_scan_datacache: cache
                .map(|cache| cache.enable_scan_datacache)
                .unwrap_or(false),
            enable_populate_datacache: cache
                .map(|cache| cache.enable_populate_datacache)
                .unwrap_or(false),
            enable_datacache_async_populate_mode: cache
                .map(|cache| cache.enable_datacache_async_populate_mode)
                .unwrap_or(false),
            enable_datacache_io_adaptor: cache
                .map(|cache| cache.enable_datacache_io_adaptor)
                .unwrap_or(false),
            enable_cache_select: cache
                .map(|cache| cache.enable_cache_select)
                .unwrap_or(false),
            datacache_evict_probability: probability,
            datacache_priority: priority,
            datacache_ttl_seconds: ttl_seconds,
            datacache_sharing_work_period: cache
                .and_then(|cache| cache.datacache_sharing_work_period),
        })
    }
}
