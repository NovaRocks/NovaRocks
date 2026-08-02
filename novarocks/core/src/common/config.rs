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
use std::path::PathBuf;

pub(crate) fn debug_exec_node_output() -> bool {
    novarocks_app_config()
        .ok()
        .map(|c| c.debug.exec_node_output)
        .unwrap_or(false)
}

pub(crate) fn debug_fault_inject_fetch_not_ready_count() -> Option<usize> {
    novarocks_app_config()
        .ok()
        .and_then(|c| c.debug.fault_inject_fetch_not_ready_count())
}

pub fn debug_emit_cancel_marker() -> bool {
    novarocks_app_config()
        .ok()
        .map(|c| c.debug.emit_cancel_marker())
        .unwrap_or(false)
        || sql_test_fragment_failure_harness_enabled()
}

pub(crate) fn debug_emit_grpc_fragment_marker() -> bool {
    novarocks_app_config()
        .ok()
        .map(|c| c.debug.emit_grpc_fragment_marker())
        .unwrap_or(false)
        || sql_test_fragment_failure_harness_enabled()
}

/// Returns whether execution should emit connector-reader evidence markers.
/// Backend native plan decoding uses this without accessing runtime state.
pub fn debug_emit_connector_reader_marker() -> bool {
    novarocks_app_config()
        .ok()
        .map(|c| c.debug.emit_connector_reader_marker())
        .unwrap_or(false)
}

pub(crate) fn sql_test_fragment_failure_harness_enabled() -> bool {
    std::env::var_os("NOVAROCKS_SQL_TEST_FRAGMENT_FAILURE_TRIGGER_FILE").is_some()
}

pub fn sql_test_query_lifecycle_fault_dir() -> Option<PathBuf> {
    novarocks_app_config()
        .ok()
        .and_then(|config| config.debug.query_lifecycle_fault_dir())
        .map(PathBuf::from)
}

pub fn exchange_wait_ms() -> u64 {
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

pub(crate) fn query_control_heartbeat_interval_ms() -> u64 {
    novarocks_app_config()
        .ok()
        .map(|config| config.runtime.query_control_heartbeat_interval_ms)
        .unwrap_or(1_000)
}

pub(crate) fn query_control_heartbeat_timeout_ms() -> u64 {
    novarocks_app_config()
        .ok()
        .map(|config| config.runtime.query_control_heartbeat_timeout_ms)
        .unwrap_or(5_000)
}

pub(crate) fn query_control_init_rpc_timeout_ms() -> u64 {
    novarocks_app_config()
        .ok()
        .map(|config| config.runtime.query_control_init_rpc_timeout_ms)
        .unwrap_or(5_000)
}

pub(crate) fn query_control_attach_timeout_ms() -> u64 {
    novarocks_app_config()
        .ok()
        .map(|config| config.runtime.query_control_attach_timeout_ms)
        .unwrap_or(5_000)
}

pub(crate) fn query_control_stage_rpc_timeout_ms() -> u64 {
    novarocks_app_config()
        .ok()
        .map(|config| config.runtime.query_control_stage_rpc_timeout_ms)
        .unwrap_or(5_000)
}

pub(crate) fn query_control_start_rpc_timeout_ms() -> u64 {
    novarocks_app_config()
        .ok()
        .map(|config| config.runtime.query_control_start_rpc_timeout_ms)
        .unwrap_or(2_000)
}

pub(crate) fn query_control_pre_start_timeout_ms() -> u64 {
    novarocks_app_config()
        .ok()
        .map(|config| config.runtime.query_control_pre_start_timeout_ms)
        .unwrap_or(30_000)
}

pub(crate) fn query_control_tombstone_retention_ms() -> u64 {
    novarocks_app_config()
        .ok()
        .map(|config| config.runtime.query_control_tombstone_retention_ms)
        .unwrap_or(120_000)
}

pub(crate) fn query_control_tombstone_capacity() -> usize {
    novarocks_app_config()
        .ok()
        .map(|config| config.runtime.query_control_tombstone_capacity)
        .unwrap_or(16_384)
}

pub(crate) fn query_control_max_active_entries() -> usize {
    novarocks_app_config()
        .ok()
        .map(|config| config.runtime.query_control_max_active_entries)
        .unwrap_or(4_096)
}

pub(crate) fn optimizer_query_mem_limit_bytes() -> u64 {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.optimizer_query_mem_limit_bytes)
        .unwrap_or(2 * 1024 * 1024 * 1024)
}

pub(crate) fn optimizer_effective_backend_count() -> u64 {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.optimizer_effective_backend_count)
        .unwrap_or(0)
}

pub(crate) fn table_schema_service_max_retries() -> usize {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.table_schema_service_max_retries.max(1))
        .unwrap_or(3)
}

pub(crate) fn table_schema_service_cache_capacity() -> u64 {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.table_schema_service_cache_capacity.max(1))
        .unwrap_or(4_096)
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

pub(crate) fn enable_tablet_write_log() -> bool {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.enable_tablet_write_log)
        .unwrap_or(false)
}

pub(crate) fn tablet_write_log_buffer_size() -> usize {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.tablet_write_log_buffer_size.max(1))
        .unwrap_or(100_000)
}

pub(crate) fn be_txn_info_history_size() -> usize {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.be_txn_info_history_size.max(1))
        .unwrap_or(20_000)
}

pub(crate) fn data_runtime_worker_threads() -> usize {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.actual_data_runtime_threads())
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        })
}

pub(crate) fn data_runtime_max_blocking_threads() -> usize {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.data_runtime_max_blocking_threads.max(1))
        .unwrap_or(64)
}

pub(crate) fn sink_io_worker_threads() -> usize {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.execution_services.actual_sink_io_worker_threads())
        .unwrap_or_else(|| {
            let cores = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1);
            cores.clamp(1, 4)
        })
}

pub(crate) fn sink_io_max_blocking_threads() -> usize {
    novarocks_app_config()
        .ok()
        .map(|c| {
            c.runtime
                .execution_services
                .sink_io_max_blocking_threads
                .max(1)
        })
        .unwrap_or(16)
}

// Consumed when the async sink operator is constructed in production (IW-3 cutover).
#[allow(dead_code)]
pub(crate) fn async_sink_queue_capacity() -> usize {
    novarocks_app_config()
        .ok()
        .map(|c| {
            c.runtime
                .execution_services
                .async_sink_queue_capacity
                .max(1)
        })
        .unwrap_or(8)
}

pub(crate) fn connector_io_tasks_per_scan_operator_default() -> i32 {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.connector_io_tasks_per_scan_operator)
        .unwrap_or(16)
}

pub(crate) fn io_coalesce_read_enable() -> bool {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.io_coalesce_read_enable)
        .unwrap_or(true)
}

pub(crate) fn io_coalesce_read_max_buffer_size() -> u64 {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.io_coalesce_read_max_buffer_size)
        .unwrap_or(8 * 1024 * 1024)
}

pub(crate) fn io_coalesce_read_max_distance_size() -> u64 {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.io_coalesce_read_max_distance_size)
        .unwrap_or(1024 * 1024)
}

pub(crate) fn io_coalesce_adaptive_lazy_active() -> bool {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.io_coalesce_adaptive_lazy_active)
        .unwrap_or(true)
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

pub(crate) fn grpc_port() -> u16 {
    novarocks_app_config()
        .ok()
        .map(|c| c.server.grpc_port)
        .unwrap_or(9080)
}
