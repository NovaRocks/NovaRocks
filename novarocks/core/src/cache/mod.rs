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
use novarocks_execution::runtime::query_options::QueryOptions;

/// Returns whether the process currently has a data-cache block store.
///
/// Protocol adapters may snapshot this capability while decoding a request, but
/// must not retain or access the cache manager itself.
pub fn datacache_block_cache_available() -> bool {
    novarocks_fs::DataCacheManager::instance()
        .block_cache()
        .is_some()
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ExternalDataCacheRangeOptions {
    pub modification_time: Option<i64>,
    pub enable_populate_datacache: Option<bool>,
    pub datacache_priority: Option<i32>,
    pub candidate_node: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CacheOptions {
    pub enable_scan_datacache: bool,
    pub enable_populate_datacache: bool,
    pub enable_datacache_async_populate_mode: bool,
    pub enable_datacache_io_adaptor: bool,
    pub enable_cache_select: bool,
    pub datacache_evict_probability: i32,
    pub datacache_priority: i32,
    pub datacache_ttl_seconds: i64,
    // Optional until FE includes the session variable in query options.
    pub datacache_sharing_work_period: Option<i64>,
}

impl CacheOptions {
    pub fn from_query_options(query_opts: Option<&QueryOptions>) -> Result<Self, String> {
        // Align with StarRocks BE semantics: only honor cache switches when FE explicitly
        // carries the corresponding query option field.
        let opts = query_opts;
        Ok(Self {
            enable_scan_datacache: opts.map(|v| v.cache.enable_scan_datacache).unwrap_or(false),
            enable_populate_datacache: opts
                .map(|v| v.cache.enable_populate_datacache)
                .unwrap_or(false),
            enable_datacache_async_populate_mode: opts
                .map(|v| v.cache.enable_datacache_async_populate_mode)
                .unwrap_or(false),
            enable_datacache_io_adaptor: opts
                .map(|v| v.cache.enable_datacache_io_adaptor)
                .unwrap_or(false),
            enable_cache_select: opts.map(|v| v.cache.enable_cache_select).unwrap_or(false),
            datacache_evict_probability: require_evict_probability(
                "datacache_evict_probability",
                opts.and_then(|v| v.cache.datacache_evict_probability)
                    .unwrap_or(100),
            )?,
            datacache_priority: parse_datacache_priority(
                "datacache_priority",
                opts.and_then(|v| v.cache.datacache_priority).unwrap_or(0),
            )?,
            datacache_ttl_seconds: parse_non_negative_i64(
                "datacache_ttl_seconds",
                opts.and_then(|v| v.cache.datacache_ttl_seconds)
                    .unwrap_or(0),
            )?,
            datacache_sharing_work_period: opts.and_then(|v| v.cache.datacache_sharing_work_period),
        })
    }

    pub fn to_file_cache_options(&self) -> novarocks_fs::CacheOptions {
        novarocks_fs::CacheOptions {
            enable_scan_datacache: self.enable_scan_datacache,
            enable_populate_datacache: self.enable_populate_datacache,
            enable_datacache_async_populate_mode: self.enable_datacache_async_populate_mode,
            enable_datacache_io_adaptor: self.enable_datacache_io_adaptor,
            enable_cache_select: self.enable_cache_select,
            datacache_evict_probability: self.datacache_evict_probability,
            datacache_priority: self.datacache_priority,
            datacache_ttl_seconds: self.datacache_ttl_seconds,
            datacache_sharing_work_period: self.datacache_sharing_work_period,
        }
    }

    pub fn with_external_range_options(
        &self,
        range_options: Option<&ExternalDataCacheRangeOptions>,
    ) -> Result<Self, String> {
        let file_range_options =
            range_options.map(novarocks_fs::ExternalDataCacheRangeOptions::from);
        let effective = self
            .to_file_cache_options()
            .with_external_range_options(file_range_options.as_ref())?;
        Ok(Self::from_file_cache_options(effective))
    }

    pub fn disable_external_datacache(&mut self) {
        self.enable_scan_datacache = false;
        self.enable_populate_datacache = false;
        self.enable_datacache_async_populate_mode = false;
        self.enable_datacache_io_adaptor = false;
    }

    fn from_file_cache_options(options: novarocks_fs::CacheOptions) -> Self {
        Self {
            enable_scan_datacache: options.enable_scan_datacache,
            enable_populate_datacache: options.enable_populate_datacache,
            enable_datacache_async_populate_mode: options.enable_datacache_async_populate_mode,
            enable_datacache_io_adaptor: options.enable_datacache_io_adaptor,
            enable_cache_select: options.enable_cache_select,
            datacache_evict_probability: options.datacache_evict_probability,
            datacache_priority: options.datacache_priority,
            datacache_ttl_seconds: options.datacache_ttl_seconds,
            datacache_sharing_work_period: options.datacache_sharing_work_period,
        }
    }
}

impl From<CacheOptions> for novarocks_fs::CacheOptions {
    fn from(options: CacheOptions) -> Self {
        options.to_file_cache_options()
    }
}

impl From<&CacheOptions> for novarocks_fs::CacheOptions {
    fn from(options: &CacheOptions) -> Self {
        options.to_file_cache_options()
    }
}

impl From<&ExternalDataCacheRangeOptions> for novarocks_fs::ExternalDataCacheRangeOptions {
    fn from(options: &ExternalDataCacheRangeOptions) -> Self {
        Self {
            modification_time: options.modification_time,
            enable_populate_datacache: options.enable_populate_datacache,
            datacache_priority: options.datacache_priority,
            candidate_node: options.candidate_node.clone(),
        }
    }
}

fn require_evict_probability(name: &str, prob: i32) -> Result<i32, String> {
    if !(0..=100).contains(&prob) {
        return Err(format!(
            "invalid query option: {name} must be in [0, 100], got {prob}"
        ));
    }
    Ok(prob)
}

fn parse_datacache_priority(name: &str, value: i32) -> Result<i32, String> {
    if !(-1..=127).contains(&value) {
        return Err(format!(
            "invalid query option: {name} must be in [-1, 127], got {value}"
        ));
    }
    Ok(value)
}

fn parse_non_negative_i64(name: &str, value: i64) -> Result<i64, String> {
    if value < 0 {
        return Err(format!(
            "invalid query option: {name} must be >= 0, got {value}"
        ));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::CacheOptions;
    use novarocks_execution::runtime::query_options::{QueryCacheOptions, QueryOptions};

    #[test]
    fn cache_switches_are_disabled_when_query_options_missing() {
        let opts = CacheOptions::from_query_options(None).expect("parse cache options");
        assert!(!opts.enable_scan_datacache);
        assert!(!opts.enable_populate_datacache);
        assert!(!opts.enable_datacache_async_populate_mode);
        assert!(!opts.enable_datacache_io_adaptor);
        assert!(!opts.enable_cache_select);
    }

    #[test]
    fn cache_switches_follow_explicit_query_options() {
        let query_opts = QueryOptions {
            cache: QueryCacheOptions {
                enable_scan_datacache: true,
                enable_populate_datacache: true,
                enable_datacache_async_populate_mode: true,
                enable_datacache_io_adaptor: true,
                enable_cache_select: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let opts =
            CacheOptions::from_query_options(Some(&query_opts)).expect("parse cache options");
        assert!(opts.enable_scan_datacache);
        assert!(opts.enable_populate_datacache);
        assert!(opts.enable_datacache_async_populate_mode);
        assert!(opts.enable_datacache_io_adaptor);
        assert!(opts.enable_cache_select);
    }
}
