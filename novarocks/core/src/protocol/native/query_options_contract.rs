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

//! Query-lifecycle-owned native query-options contract decoding.
//!
//! This deliberately sits outside fragment decoding: participant manifests
//! must be validated before a backend owns or decodes a fragment.

use crate::exec::spill::{SpillConfig, SpillMode};
use crate::protocol::common::error::{FieldPath, ProtocolError, ProtocolErrorKind, ProtocolFamily};
use novarocks_execution::runtime::query_options::{QueryCacheOptions, QueryOptions};
use novarocks_protocol::novarocks;

pub(crate) fn decode_query_options(
    src: &novarocks::QueryOptions,
) -> Result<QueryOptions, ProtocolError> {
    let path = FieldPath::root("instance_params").field("query_options");
    Ok(QueryOptions {
        batch_size: (src.batch_size > 0).then_some(src.batch_size),
        query_timeout: (src.query_timeout > 0).then_some(src.query_timeout),
        query_delivery_timeout: (src.query_delivery_timeout > 0)
            .then_some(src.query_delivery_timeout),
        enable_profile: src.enable_profile,
        runtime_profile_report_interval: (src.runtime_profile_report_interval > 0)
            .then_some(src.runtime_profile_report_interval),
        pipeline_dop: (src.pipeline_dop > 0).then_some(src.pipeline_dop),
        exec_mem_limit: (src.query_mem_limit > 0).then_some(src.query_mem_limit),
        connector_io_tasks_per_scan_operator: (src.connector_io_tasks_per_scan_operator > 0)
            .then_some(src.connector_io_tasks_per_scan_operator),
        orc_use_column_names: src.orc_use_column_names,
        enable_file_metacache: src.enable_file_metacache,
        enable_file_pagecache: src.enable_file_pagecache,
        enable_parquet_reader_page_index: src.enable_parquet_reader_page_index,
        runtime_filter_scan_wait_time_ms: src.runtime_filter_scan_wait_time_ms,
        runtime_filter_wait_timeout_ms: src.runtime_filter_wait_timeout_ms,
        allow_throw_exception: src.allow_throw_exception,
        group_concat_max_len: src.group_concat_max_len,
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
        spill: decode_spill_config(src, path.field("spill_options"))?,
    })
}

fn decode_spill_config(
    src: &novarocks::QueryOptions,
    path: FieldPath,
) -> Result<Option<SpillConfig>, ProtocolError> {
    if !src.enable_spill {
        return Ok(None);
    }
    let spill = src.spill_options.as_ref().ok_or_else(|| {
        protocol_error(
            path.clone(),
            ProtocolErrorKind::MissingField,
            "enable_spill=true requires spill_options",
        )
    })?;
    let spill_mode = match spill.spill_mode {
        0 => SpillMode::Auto,
        1 => SpillMode::Force,
        2 => SpillMode::None,
        3 => SpillMode::Random,
        value => {
            return Err(protocol_error(
                path.clone().field("spill_mode"),
                ProtocolErrorKind::InvalidEnum,
                format!("unknown spill_mode value {value}"),
            ));
        }
    };
    if spill_mode == SpillMode::Random {
        return Err(protocol_error(
            path.field("spill_mode"),
            ProtocolErrorKind::InvalidValue,
            "spill_mode RANDOM is not supported yet",
        ));
    }
    Ok(Some(SpillConfig {
        enable_spill: true,
        spill_mode,
        spill_mem_limit_threshold: (spill.spill_mem_limit_threshold > 0.0)
            .then_some(spill.spill_mem_limit_threshold),
        spill_operator_min_bytes: (spill.spill_operator_min_bytes > 0)
            .then_some(spill.spill_operator_min_bytes),
        spill_operator_max_bytes: (spill.spill_operator_max_bytes > 0)
            .then_some(spill.spill_operator_max_bytes),
        spill_encode_level: (spill.spill_encode_level > 0).then_some(spill.spill_encode_level),
        enable_spill_buffer_read: Some(spill.enable_spill_buffer_read),
        max_spill_read_buffer_bytes_per_driver: (spill.max_spill_read_buffer_bytes_per_driver > 0)
            .then_some(spill.max_spill_read_buffer_bytes_per_driver),
        spill_mem_table_size: (spill.spill_mem_table_size > 0)
            .then_some(spill.spill_mem_table_size),
        spill_mem_table_num: (spill.spill_mem_table_num > 0).then_some(spill.spill_mem_table_num),
    }))
}

fn protocol_error(
    path: FieldPath,
    kind: ProtocolErrorKind,
    detail: impl Into<String>,
) -> ProtocolError {
    ProtocolError::new(ProtocolFamily::Native, path, kind, detail)
}
