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
use std::sync::OnceLock;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};

use crate::common::config;
use crate::common::types::UniqueId;
use crate::exec::spill::{QuerySpillManager, SpillConfig};
use crate::internal_service;
use crate::novarocks_logging::debug;
use crate::runtime::mem_tracker::{self, MemTracker};
use crate::runtime::profile::clamp_u128_to_i64;
use crate::runtime::query_context::QueryId;
use crate::runtime::sink_commit;
use crate::runtime_filter;

/// RuntimeState is a per-fragment-instance execution context, similar to StarRocks BE RuntimeState.
///
/// Today it mainly provides access to frequently used query options (e.g. `batch_size` / chunk size).
/// More execution-time parameters and state can be migrated here over time.
#[derive(Debug)]
pub struct RuntimeState {
    query_options: Option<internal_service::TQueryOptions>,
    cache_options: Option<crate::cache::CacheOptions>,
    error_state: std::sync::Arc<RuntimeErrorState>,
    last_report_exec_state_ns: AtomicI64,
    query_id: Option<QueryId>,
    runtime_filter_params: Option<runtime_filter::TRuntimeFilterParams>,
    fragment_instance_id: Option<UniqueId>,
    backend_num: Option<i32>,
    mem_tracker: Option<std::sync::Arc<MemTracker>>,
    spill_config: Option<SpillConfig>,
    spill_manager: Option<std::sync::Arc<QuerySpillManager>>,
}

#[derive(Debug, Default)]
pub struct RuntimeErrorState {
    error: std::sync::Mutex<Option<String>>,
}

impl RuntimeErrorState {
    pub fn set_error(&self, err: String) {
        let mut guard = self.error.lock().expect("runtime error lock");
        if guard.is_none() {
            *guard = Some(err);
        }
    }

    pub fn error(&self) -> Option<String> {
        self.error.lock().expect("runtime error lock").clone()
    }
}

impl Default for RuntimeState {
    fn default() -> Self {
        Self {
            query_options: None,
            cache_options: None,
            error_state: std::sync::Arc::new(RuntimeErrorState::default()),
            last_report_exec_state_ns: AtomicI64::new(0),
            query_id: None,
            runtime_filter_params: None,
            fragment_instance_id: None,
            backend_num: None,
            mem_tracker: None,
            spill_config: None,
            spill_manager: None,
        }
    }
}

impl Clone for RuntimeState {
    fn clone(&self) -> Self {
        Self {
            query_options: self.query_options.clone(),
            cache_options: self.cache_options.clone(),
            error_state: std::sync::Arc::clone(&self.error_state),
            last_report_exec_state_ns: AtomicI64::new(
                self.last_report_exec_state_ns.load(Ordering::Acquire),
            ),
            query_id: self.query_id,
            runtime_filter_params: self.runtime_filter_params.clone(),
            fragment_instance_id: self.fragment_instance_id,
            backend_num: self.backend_num,
            mem_tracker: self.mem_tracker.clone(),
            spill_config: self.spill_config.clone(),
            spill_manager: self.spill_manager.clone(),
        }
    }
}

impl RuntimeState {
    pub(crate) fn new(
        query_options: Option<internal_service::TQueryOptions>,
        cache_options: Option<crate::cache::CacheOptions>,
        query_id: Option<QueryId>,
        runtime_filter_params: Option<runtime_filter::TRuntimeFilterParams>,
        fragment_instance_id: Option<UniqueId>,
        backend_num: Option<i32>,
        mem_tracker: Option<std::sync::Arc<MemTracker>>,
        spill_config: Option<SpillConfig>,
        spill_manager: Option<std::sync::Arc<QuerySpillManager>>,
    ) -> Self {
        let mem_tracker = mem_tracker.or_else(|| {
            if query_id.is_none() && fragment_instance_id.is_none() {
                return None;
            }
            let process = mem_tracker::process_mem_tracker();
            let query_label = query_id
                .map(|id| format!("query_{:x}_{:x}", id.hi, id.lo))
                .unwrap_or_else(|| "query_unknown".to_string());
            let query_tracker = MemTracker::new_child(query_label, &process);
            let fragment_label = fragment_instance_id
                .map(|id| format!("fragment_{:x}_{:x}", id.hi, id.lo))
                .unwrap_or_else(|| "fragment_unknown".to_string());
            Some(MemTracker::new_child(fragment_label, &query_tracker))
        });
        let state = Self {
            query_options,
            cache_options,
            error_state: std::sync::Arc::new(RuntimeErrorState::default()),
            last_report_exec_state_ns: AtomicI64::new(0),
            query_id,
            runtime_filter_params,
            fragment_instance_id,
            backend_num,
            mem_tracker,
            spill_config,
            spill_manager,
        };
        if let Some(finst_id) = fragment_instance_id {
            sink_commit::register(finst_id);
        }
        state
    }

    #[allow(dead_code)]
    pub fn query_options(&self) -> Option<&internal_service::TQueryOptions> {
        self.query_options.as_ref()
    }

    pub fn cache_options(&self) -> Option<&crate::cache::CacheOptions> {
        self.cache_options.as_ref()
    }

    pub(crate) fn query_id(&self) -> Option<QueryId> {
        self.query_id
    }

    pub(crate) fn fragment_instance_id(&self) -> Option<UniqueId> {
        self.fragment_instance_id
    }

    pub(crate) fn add_sink_commit_info(&self, info: crate::types::TSinkCommitInfo) {
        let Some(finst_id) = self.fragment_instance_id else {
            debug!(
                target: "novarocks::sink_commit",
                "skip sink_commit_info because fragment_instance_id is missing"
            );
            return;
        };
        let file_path = info
            .iceberg_data_file
            .as_ref()
            .and_then(|file| file.path.as_deref())
            .unwrap_or("");
        debug!(
            target: "novarocks::sink_commit",
            finst_id = %finst_id,
            file_path = %file_path,
            "add sink_commit_info"
        );
        sink_commit::add(finst_id, info);
    }

    pub(crate) fn add_sink_load_counters(&self, loaded_rows: i64, loaded_bytes: i64) {
        let Some(finst_id) = self.fragment_instance_id else {
            debug!(
                target: "novarocks::sink_commit",
                "skip sink load counters because fragment_instance_id is missing"
            );
            return;
        };
        if loaded_rows <= 0 && loaded_bytes <= 0 {
            return;
        }
        debug!(
            target: "novarocks::sink_commit",
            finst_id = %finst_id,
            loaded_rows,
            loaded_bytes,
            "add sink load counters"
        );
        sink_commit::add_load_counters(finst_id, loaded_rows, loaded_bytes);
    }

    pub(crate) fn add_tablet_commit_info(&self, info: crate::types::TTabletCommitInfo) {
        let Some(finst_id) = self.fragment_instance_id else {
            debug!(
                target: "novarocks::sink_commit",
                "skip tablet_commit_info because fragment_instance_id is missing"
            );
            return;
        };
        debug!(
            target: "novarocks::sink_commit",
            finst_id = %finst_id,
            tablet_id = info.tablet_id,
            backend_id = info.backend_id,
            "add tablet_commit_info"
        );
        sink_commit::add_tablet_commit_info(finst_id, info);
    }

    pub(crate) fn add_tablet_commit_infos(
        &self,
        infos: impl IntoIterator<Item = crate::types::TTabletCommitInfo>,
    ) {
        for info in infos {
            self.add_tablet_commit_info(info);
        }
    }

    pub(crate) fn add_tablet_fail_info(&self, info: crate::types::TTabletFailInfo) {
        let Some(finst_id) = self.fragment_instance_id else {
            debug!(
                target: "novarocks::sink_commit",
                "skip tablet_fail_info because fragment_instance_id is missing"
            );
            return;
        };
        debug!(
            target: "novarocks::sink_commit",
            finst_id = %finst_id,
            tablet_id = ?info.tablet_id,
            backend_id = ?info.backend_id,
            "add tablet_fail_info"
        );
        sink_commit::add_tablet_fail_info(finst_id, info);
    }

    pub(crate) fn add_tablet_fail_infos(
        &self,
        infos: impl IntoIterator<Item = crate::types::TTabletFailInfo>,
    ) {
        for info in infos {
            self.add_tablet_fail_info(info);
        }
    }

    pub(crate) fn mem_tracker(&self) -> Option<std::sync::Arc<MemTracker>> {
        self.mem_tracker.clone()
    }

    pub(crate) fn backend_num(&self) -> Option<i32> {
        self.backend_num
    }

    pub(crate) fn spill_config(&self) -> Option<&SpillConfig> {
        self.spill_config.as_ref()
    }

    pub(crate) fn spill_manager(&self) -> Option<std::sync::Arc<QuerySpillManager>> {
        self.spill_manager.clone()
    }

    pub(crate) fn runtime_filter_params(&self) -> Option<&runtime_filter::TRuntimeFilterParams> {
        self.runtime_filter_params.as_ref()
    }

    pub(crate) fn runtime_filter_scan_wait_timeout(&self) -> Option<Duration> {
        let opts = self.query_options.as_ref();
        let scan_wait = opts
            .and_then(|opts| opts.runtime_filter_scan_wait_time_ms)
            .and_then(|v| (v >= 0).then_some(v as i64));
        let override_scan =
            config::runtime_filter_scan_wait_time_ms_override().and_then(|v| (v >= 0).then_some(v));
        let ms = override_scan.or(scan_wait)?;
        Some(Duration::from_millis(ms as u64))
    }

    pub(crate) fn runtime_filter_wait_timeout(&self) -> Option<Duration> {
        const DEFAULT_WAIT_TIMEOUT_MS: i64 = 1000;
        let opts = self.query_options.as_ref();
        let wait_timeout = opts
            .and_then(|opts| opts.runtime_filter_wait_timeout_ms)
            .and_then(|v| (v >= 0).then_some(v as i64));
        let override_wait =
            config::runtime_filter_wait_timeout_ms_override().and_then(|v| (v >= 0).then_some(v));
        let ms = override_wait
            .or(wait_timeout)
            .unwrap_or(DEFAULT_WAIT_TIMEOUT_MS);
        Some(Duration::from_millis(ms as u64))
    }

    pub fn error_state(&self) -> std::sync::Arc<RuntimeErrorState> {
        std::sync::Arc::clone(&self.error_state)
    }

    pub fn error(&self) -> Option<String> {
        self.error_state.error()
    }

    /// Return the maximum row count per in-memory chunk/RecordBatch.
    ///
    /// StarRocks BE uses `TQueryOptions.batch_size` (aka `RuntimeState::chunk_size()`).
    pub fn chunk_size(&self) -> usize {
        self.query_options
            .as_ref()
            .and_then(|opts| opts.batch_size)
            .filter(|v| *v > 0)
            .map(|v| v as usize)
            .unwrap_or(4096)
            .max(1)
    }

    pub fn runtime_profile_report_interval_ns(&self) -> Option<i64> {
        let opts = self.query_options.as_ref()?;
        if !opts.enable_profile.unwrap_or(false) {
            return None;
        }
        let interval_s = opts.runtime_profile_report_interval.unwrap_or(0);
        if interval_s <= 0 {
            return None;
        }
        Some(interval_s.saturating_mul(1_000_000_000))
    }

    pub fn should_report_exec_state(&self) -> bool {
        let Some(interval_ns) = self.runtime_profile_report_interval_ns() else {
            return false;
        };
        let now_ns = monotonic_now_ns();
        let last_ns = self.last_report_exec_state_ns.load(Ordering::Acquire);
        if now_ns.saturating_sub(last_ns) < interval_ns {
            return false;
        }
        self.last_report_exec_state_ns
            .compare_exchange(last_ns, now_ns, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}

fn monotonic_now_ns() -> i64 {
    static START: OnceLock<Instant> = OnceLock::new();
    let start = START.get_or_init(Instant::now);
    clamp_u128_to_i64(start.elapsed().as_nanos())
}
