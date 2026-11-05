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

mod block_cache;
mod cache_input_stream;
mod data_cache;
mod page_cache;
pub(crate) mod parquet_cache;

pub use block_cache::{BlockCache, BlockCacheOptions, CacheKey, get_block_cache, init_block_cache};
pub use cache_input_stream::{CacheBlockRead, CacheInputStream};
pub use data_cache::{
    DataCacheManager, DataCacheMetricsRecorder, DataCachePageCache, DataCachePageCacheOptions,
    DataCachePageKey, DataCachePolicy,
};
pub use page_cache::{PageCache, PageCacheStats, PageCacheValue, PagePin};
pub use parquet_cache::{ParquetCacheOptions, init_parquet_cache};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheDomain {
    External,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataCacheIoOptions {
    pub domain: CacheDomain,
    pub enable_datacache: bool,
    pub enable_populate_datacache: bool,
    pub enable_datacache_async_populate_mode: bool,
    pub enable_datacache_io_adaptor: bool,
    pub enable_cache_select: bool,
    pub datacache_evict_probability: i32,
    pub datacache_priority: i32,
    pub datacache_ttl_seconds: i64,
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
    pub datacache_sharing_work_period: Option<i64>,
}

impl CacheOptions {
    pub fn with_external_range_options(
        &self,
        range_options: Option<&ExternalDataCacheRangeOptions>,
    ) -> Result<Self, String> {
        let mut effective = self.clone();
        if let Some(range_options) = range_options {
            if let Some(enable_populate_datacache) = range_options.enable_populate_datacache {
                effective.enable_populate_datacache = enable_populate_datacache;
            }
            if let Some(datacache_priority) = range_options.datacache_priority {
                effective.datacache_priority = validate_datacache_priority(
                    "external range datacache priority",
                    datacache_priority,
                )?;
            }
        }
        if effective.datacache_priority == -1 {
            effective.enable_scan_datacache = false;
        }
        if !effective.enable_scan_datacache {
            effective.disable_external_datacache();
        }
        Ok(effective)
    }

    pub fn disable_external_datacache(&mut self) {
        self.enable_scan_datacache = false;
        self.enable_populate_datacache = false;
        self.enable_datacache_async_populate_mode = false;
        self.enable_datacache_io_adaptor = false;
    }

    pub fn external_io_options(&self) -> DataCacheIoOptions {
        DataCacheIoOptions {
            domain: CacheDomain::External,
            enable_datacache: self.enable_scan_datacache,
            enable_populate_datacache: self.enable_populate_datacache,
            enable_datacache_async_populate_mode: self.enable_datacache_async_populate_mode,
            enable_datacache_io_adaptor: self.enable_datacache_io_adaptor,
            enable_cache_select: self.enable_cache_select,
            datacache_evict_probability: self.datacache_evict_probability,
            datacache_priority: self.datacache_priority,
            datacache_ttl_seconds: self.datacache_ttl_seconds,
        }
    }
}

pub fn validate_evict_probability(name: &str, probability: i32) -> Result<i32, String> {
    if !(0..=100).contains(&probability) {
        return Err(format!(
            "invalid cache option: {name} must be in [0, 100], got {probability}"
        ));
    }
    Ok(probability)
}

pub fn validate_datacache_priority(name: &str, value: i32) -> Result<i32, String> {
    if !(-1..=127).contains(&value) {
        return Err(format!(
            "invalid cache option: {name} must be in [-1, 127], got {value}"
        ));
    }
    Ok(value)
}

pub fn validate_non_negative_i64(name: &str, value: i64) -> Result<i64, String> {
    if value < 0 {
        return Err(format!(
            "invalid cache option: {name} must be >= 0, got {value}"
        ));
    }
    Ok(value)
}
