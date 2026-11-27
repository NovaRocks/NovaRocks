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

//! Task-local creation facts the native encoder derives per placement.
//!
//! A task's per-instance facts no longer travel as a copy of the execution
//! kernel's instance parameters. The descriptor owns identity, kernel key,
//! parallelism and topology, the established context owns query-wide
//! options, and the task assignment owns what neither can know. This module
//! keeps only the two derivations the frontend owns: a fragment's task width
//! and the query-wide options an establish carries.

use novarocks_execution::runtime::query_options::QueryOptions;
use novarocks_physical_plan::PipelineDopDomain;
use novarocks_proto_models::novarocks as wire;

/// Choose the largest allowed task width no greater than the query's requested
/// width. A whole-relation fragment can require one driver even when another
/// fragment in the same query is allowed to use the full query width.
pub(crate) fn select_fragment_pipeline_dop(
    domain: PipelineDopDomain,
    requested: i32,
) -> Result<i32, String> {
    let requested = u32::try_from(requested)
        .ok()
        .filter(|dop| *dop > 0)
        .ok_or_else(|| {
            "query pipeline DOP must be positive before fragment submission".to_owned()
        })?;
    if domain.min == 0 || domain.max < domain.min {
        return Err("fragment pipeline DOP domain is invalid".to_owned());
    }
    let capped = requested.min(domain.max);
    let selected = if domain.requires_power_of_two {
        1_u32 << capped.ilog2()
    } else {
        capped
    };
    if selected < domain.min {
        return Err(format!(
            "fragment pipeline DOP domain {}..={} cannot satisfy requested DOP {requested}",
            domain.min, domain.max
        ));
    }
    i32::try_from(selected)
        .map_err(|_| "selected fragment pipeline DOP exceeds wire width".to_owned())
}

/// FE-owned execution settings are projected into the generated native
/// carrier at the fragment submission boundary. The protocol representation
/// deliberately does not own runtime defaults or execution types.
pub(crate) fn encode_query_options(src: &QueryOptions) -> wire::QueryOptions {
    wire::QueryOptions {
        query_timeout: src.query_timeout.unwrap_or_default(),
        enable_profile: src.enable_profile,
        pipeline_dop: src.pipeline_dop.unwrap_or_default(),
        query_mem_limit: src.exec_mem_limit.unwrap_or_default(),
        runtime_filter_scan_wait_time_ms: src.runtime_filter_scan_wait_time_ms,
        runtime_filter_wait_timeout_ms: src.runtime_filter_wait_timeout_ms,

        enable_scan_datacache: src.cache.enable_scan_datacache,
        enable_populate_datacache: src.cache.enable_populate_datacache,
        enable_datacache_async_populate_mode: src.cache.enable_datacache_async_populate_mode,
        enable_datacache_io_adaptor: src.cache.enable_datacache_io_adaptor,
        enable_cache_select: src.cache.enable_cache_select,
        datacache_evict_probability: src.cache.datacache_evict_probability,
        datacache_priority: src.cache.datacache_priority.unwrap_or_default(),
        datacache_ttl_seconds: src.cache.datacache_ttl_seconds.unwrap_or_default(),
        datacache_sharing_work_period: src.cache.datacache_sharing_work_period.unwrap_or_default(),
        query_delivery_timeout: src.query_delivery_timeout.unwrap_or_default(),
        runtime_profile_report_interval: src.runtime_profile_report_interval.unwrap_or_default(),
        enable_join_runtime_bitset_filter: src.enable_join_runtime_bitset_filter,
        global_runtime_filter_build_max_size: src
            .global_runtime_filter_build_max_size
            .unwrap_or_default(),
        orc_use_column_names: src.orc_use_column_names,
        enable_file_metacache: src.enable_file_metacache,
        enable_file_pagecache: src.enable_file_pagecache,
        enable_parquet_reader_page_index: src.enable_parquet_reader_page_index,
    }
}
