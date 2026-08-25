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

//! Deterministic instance sidecar-to-protobuf mapping for the native boundary.

use std::collections::HashMap;

use crate::query_execution::FragmentInstancePlacement;
use novarocks_execution::exec::spill::{SpillConfig, SpillMode};
use novarocks_execution::runtime::endpoint::FragmentDestination;
use novarocks_execution::runtime::query_options::QueryOptions;
use novarocks_proto::{common, novarocks as wire};
use novarocks_types::UniqueId;

pub(crate) fn encode_instance_params(
    query_id: &UniqueId,
    placement: &FragmentInstancePlacement,
    query_options: &QueryOptions,
    backend_num: i32,
    typed_result_sink: bool,
) -> Result<wire::InstanceParams, String> {
    Ok(wire::InstanceParams {
        query_id: Some(encode_unique_id(query_id)),
        fragment_instance_id: Some(encode_unique_id(&placement.finst_id)),
        backend_num,
        per_node_scan_ranges: placement
            .scan_ranges
            .iter()
            .map(|(node_id, ranges)| {
                Ok((
                    *node_id,
                    wire::ScanRangeList {
                        ranges: ranges
                            .iter()
                            .map(|range| range.as_proto().clone())
                            .collect(),
                    },
                ))
            })
            .collect::<Result<HashMap<_, _>, String>>()?,
        per_exch_num_senders: placement
            .per_exch_num_senders
            .iter()
            .map(|(node_id, senders)| (*node_id, *senders))
            .collect(),
        destinations: placement
            .destinations
            .iter()
            .map(encode_destination)
            .collect::<Result<Vec<_>, _>>()?,
        query_options: Some(encode_query_options(query_options)),
        typed_result_sink,
    })
}

fn encode_unique_id(src: &UniqueId) -> common::UniqueId {
    common::UniqueId {
        hi: src.high(),
        lo: src.low(),
    }
}

/// FE-owned execution settings are projected into the generated native
/// carrier at the fragment submission boundary. Protocol validates the wire
/// carrier, but deliberately does not own runtime defaults or execution types.
pub(crate) fn encode_query_options(src: &QueryOptions) -> wire::QueryOptions {
    wire::QueryOptions {
        batch_size: src.batch_size.unwrap_or_default(),
        query_timeout: src.query_timeout.unwrap_or_default(),
        enable_profile: src.enable_profile,
        pipeline_dop: src.pipeline_dop.unwrap_or_default(),
        query_mem_limit: src.exec_mem_limit.unwrap_or_default(),
        connector_io_tasks_per_scan_operator: src
            .connector_io_tasks_per_scan_operator
            .unwrap_or_default(),
        runtime_filter_scan_wait_time_ms: src.runtime_filter_scan_wait_time_ms,
        runtime_filter_wait_timeout_ms: src.runtime_filter_wait_timeout_ms,
        allow_throw_exception: src.allow_throw_exception,
        group_concat_max_len: src.group_concat_max_len,
        enable_spill: src.spill.is_some(),
        spill_options: src.spill.as_ref().map(encode_spill_config),
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

fn encode_spill_config(src: &SpillConfig) -> wire::SpillOptions {
    wire::SpillOptions {
        spill_mode: match src.spill_mode {
            SpillMode::Auto => 0,
            SpillMode::Force => 1,
            SpillMode::None => 2,
            SpillMode::Random => 3,
        },
        spill_mem_limit_threshold: src.spill_mem_limit_threshold.unwrap_or_default(),
        spill_operator_min_bytes: src.spill_operator_min_bytes.unwrap_or_default(),
        spill_operator_max_bytes: src.spill_operator_max_bytes.unwrap_or_default(),
        spill_encode_level: src.spill_encode_level.unwrap_or_default(),
        enable_spill_buffer_read: src.enable_spill_buffer_read.unwrap_or(false),
        max_spill_read_buffer_bytes_per_driver: src
            .max_spill_read_buffer_bytes_per_driver
            .unwrap_or_default(),
        spill_mem_table_size: src.spill_mem_table_size.unwrap_or_default(),
        spill_mem_table_num: src.spill_mem_table_num.unwrap_or_default(),
    }
}

fn encode_destination(src: &FragmentDestination) -> Result<wire::Destination, String> {
    Ok(wire::Destination {
        finst_id: Some(encode_unique_id(src.finst_id())),
        endpoint: src.endpoint().as_host_port(),
    })
}
