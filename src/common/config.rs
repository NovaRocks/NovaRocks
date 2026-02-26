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
use crate::novarocks_config::config as novarocks_app_config;

pub(crate) fn debug_exec_node_output() -> bool {
    novarocks_app_config()
        .ok()
        .map(|c| c.debug.exec_node_output)
        .unwrap_or(false)
}

pub(crate) fn debug_exec_batch_plan_json() -> bool {
    novarocks_app_config()
        .ok()
        .map(|c| c.debug.exec_batch_plan_json)
        .unwrap_or(false)
}

pub(crate) fn exchange_wait_ms() -> u64 {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.exchange_wait_ms)
        .unwrap_or(120_000)
}

pub(crate) fn exchange_max_transmit_batched_bytes() -> usize {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.exchange_max_transmit_batched_bytes)
        .unwrap_or(262_144)
}

pub(crate) fn exchange_io_threads() -> usize {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.exchange_io_threads)
        .unwrap_or(4)
}

pub(crate) fn exchange_io_max_inflight_bytes() -> usize {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.exchange_io_max_inflight_bytes)
        .unwrap_or(64 * 1024 * 1024)
}

pub(crate) fn local_exchange_buffer_mem_limit_per_driver() -> usize {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.local_exchange_buffer_mem_limit_per_driver)
        .unwrap_or(128 * 1024 * 1024)
}

pub(crate) fn local_exchange_max_buffered_rows() -> i64 {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.local_exchange_max_buffered_rows)
        .unwrap_or(-1)
}

pub(crate) fn operator_buffer_chunks() -> usize {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.operator_buffer_chunks)
        .unwrap_or(8)
}

pub(crate) fn olap_sink_write_buffer_size_bytes() -> usize {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.olap_sink_write_buffer_size_bytes)
        .unwrap_or(100 * 1024 * 1024)
}

pub(crate) fn olap_sink_max_tablet_write_chunk_bytes() -> usize {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.olap_sink_max_tablet_write_chunk_bytes)
        .unwrap_or(512 * 1024 * 1024)
}

pub(crate) fn pipeline_scan_thread_pool_thread_num() -> usize {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.actual_scan_threads())
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        })
}

pub(crate) fn connector_io_tasks_per_scan_operator_default() -> i32 {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.connector_io_tasks_per_scan_operator)
        .unwrap_or(16)
}

pub(crate) fn pipeline_scan_thread_pool_queue_size() -> usize {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.pipeline_scan_thread_pool_queue_size)
        .unwrap_or(102_400)
}

pub(crate) fn spill_io_threads() -> usize {
    novarocks_app_config()
        .ok()
        .map(|c| {
            if c.runtime.spill_io_threads > 0 {
                c.runtime.spill_io_threads
            } else {
                c.runtime.actual_exec_threads()
            }
        })
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        })
}

pub(crate) fn spill_io_queue_size() -> usize {
    novarocks_app_config()
        .ok()
        .map(|c| {
            if c.runtime.spill_io_queue_size == 0 {
                1024
            } else {
                c.runtime.spill_io_queue_size
            }
        })
        .unwrap_or(1024)
}

pub(crate) fn scan_submit_fail_max() -> usize {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.scan_submit_fail_max)
        .unwrap_or(128)
}

pub(crate) fn scan_submit_fail_timeout_ms() -> u64 {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.scan_submit_fail_timeout_ms)
        .unwrap_or(2000)
}

pub(crate) fn runtime_filter_scan_wait_time_ms_override() -> Option<i64> {
    novarocks_app_config()
        .ok()
        .and_then(|c| c.runtime.runtime_filter_scan_wait_time_ms_override)
}

pub(crate) fn runtime_filter_wait_timeout_ms_override() -> Option<i64> {
    novarocks_app_config()
        .ok()
        .and_then(|c| c.runtime.runtime_filter_wait_timeout_ms_override)
}

pub(crate) fn spill_enable() -> bool {
    novarocks_app_config()
        .ok()
        .map(|c| c.spill.enable)
        .unwrap_or(false)
}

pub(crate) fn spill_local_dirs() -> Vec<String> {
    let configured = novarocks_app_config()
        .ok()
        .map(|c| c.spill.local_dirs.clone())
        .unwrap_or_default();
    if configured.is_empty() {
        let mut default_dir = std::env::temp_dir();
        default_dir.push("novarocks-spill");
        vec![default_dir.to_string_lossy().into_owned()]
    } else {
        configured
    }
}

pub(crate) fn spill_dir_max_bytes() -> u64 {
    novarocks_app_config()
        .ok()
        .map(|c| c.spill.dir_max_bytes)
        .unwrap_or(0)
}

pub(crate) fn spill_block_size_bytes() -> u64 {
    novarocks_app_config()
        .ok()
        .map(|c| c.spill.block_size_bytes)
        .unwrap_or(134_217_728)
}

pub(crate) fn spill_ipc_compression() -> String {
    novarocks_app_config()
        .ok()
        .map(|c| c.spill.ipc_compression.clone())
        .unwrap_or_else(|| "lz4".to_string())
}

pub(crate) fn http_port() -> u16 {
    novarocks_app_config()
        .ok()
        .map(|c| c.server.http_port)
        .unwrap_or(8040)
}

pub(crate) fn starlet_port() -> u16 {
    novarocks_app_config()
        .ok()
        .map(|c| c.server.starlet_port)
        .unwrap_or(9070)
}
