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

//! Native wire reconstruction of execution query options.

use novarocks_execution::runtime::query_options::{QueryCacheOptions, QueryOptions};
use novarocks_proto_models::novarocks;

pub fn decode_query_options(src: &novarocks::QueryOptions) -> QueryOptions {
    QueryOptions {
        query_timeout: (src.query_timeout > 0).then_some(src.query_timeout),
        query_delivery_timeout: (src.query_delivery_timeout > 0)
            .then_some(src.query_delivery_timeout),
        enable_profile: src.enable_profile,
        runtime_profile_report_interval: (src.runtime_profile_report_interval > 0)
            .then_some(src.runtime_profile_report_interval),
        pipeline_dop: (src.pipeline_dop > 0).then_some(src.pipeline_dop),
        exec_mem_limit: (src.query_mem_limit > 0).then_some(src.query_mem_limit),
        orc_use_column_names: src.orc_use_column_names,
        enable_file_metacache: src.enable_file_metacache,
        enable_file_pagecache: src.enable_file_pagecache,
        enable_parquet_reader_page_index: src.enable_parquet_reader_page_index,
        runtime_filter_scan_wait_time_ms: src.runtime_filter_scan_wait_time_ms,
        runtime_filter_wait_timeout_ms: src.runtime_filter_wait_timeout_ms,
        allow_throw_exception: src.allow_throw_exception,

        enable_join_runtime_bitset_filter: src.enable_join_runtime_bitset_filter,
        global_runtime_filter_build_max_size: (src.global_runtime_filter_build_max_size > 0)
            .then_some(src.global_runtime_filter_build_max_size),
        cache: QueryCacheOptions {
            enable_scan_datacache: src.enable_scan_datacache,
            enable_populate_datacache: src.enable_populate_datacache,
            enable_datacache_async_populate_mode: src.enable_datacache_async_populate_mode,
            enable_datacache_io_adaptor: src.enable_datacache_io_adaptor,
            enable_cache_select: src.enable_cache_select,
            datacache_evict_probability: src.datacache_evict_probability,
            datacache_priority: (src.datacache_priority != 0).then_some(src.datacache_priority),
            datacache_ttl_seconds: (src.datacache_ttl_seconds > 0)
                .then_some(src.datacache_ttl_seconds),
            datacache_sharing_work_period: (src.datacache_sharing_work_period > 0)
                .then_some(src.datacache_sharing_work_period),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::decode_query_options;
    use novarocks_proto_models::novarocks;

    #[test]
    fn preserves_explicit_zero_and_absent_bitset() {
        let decoded = decode_query_options(&novarocks::QueryOptions {
            runtime_filter_scan_wait_time_ms: Some(0),
            runtime_filter_wait_timeout_ms: Some(0),

            datacache_evict_probability: Some(0),
            ..Default::default()
        });
        assert_eq!(decoded.runtime_filter_scan_wait_time_ms(), Some(0));
        assert_eq!(decoded.runtime_filter_wait_timeout_ms(), Some(0));

        assert_eq!(decoded.cache().datacache_evict_probability, Some(0));
        assert_eq!(decoded.enable_join_runtime_bitset_filter(), None);
    }
}
