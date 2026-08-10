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

use std::sync::Arc;

use crate::exec::spill::QuerySpillManager;
use crate::runtime::execution_runtime::ExecutionRuntime;
use crate::runtime::fragment::io::ScanRegistrationPort;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::Profiler;
use crate::runtime::query_options::QueryOptions;
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime_filter::RuntimeFilterSessionRef;
use novarocks_types::QueryId;
use novarocks_types::UniqueId;

pub(crate) struct RuntimeStateInputs {
    pub(crate) query_options: Option<QueryOptions>,
    pub(crate) query_id: Option<QueryId>,
    pub(crate) fragment_instance_id: Option<UniqueId>,
    pub(crate) backend_num: Option<i32>,
    pub(crate) mem_tracker: Option<Arc<MemTracker>>,
    pub(crate) runtime_filter_session: Option<RuntimeFilterSessionRef>,
    pub(crate) connector_staged_report_collector:
        Option<crate::runtime::connector_write_report::ConnectorStagedReportCollector>,
    pub(crate) execution_runtime: Option<Arc<ExecutionRuntime>>,
    pub(crate) scan_registration: Option<Arc<dyn ScanRegistrationPort>>,
}

pub(crate) fn apply_query_option_overrides(
    mut query_options: Option<QueryOptions>,
    execution_runtime: Option<&ExecutionRuntime>,
) -> Option<QueryOptions> {
    if let Some(opts) = query_options.as_mut() {
        if let Some(ms) = execution_runtime
            .and_then(|runtime| runtime.config().runtime_filter_scan_wait_time_ms_override)
        {
            opts.runtime_filter_scan_wait_time_ms = Some(ms);
        }
        if let Some(ms) = execution_runtime
            .and_then(|runtime| runtime.config().runtime_filter_wait_timeout_ms_override)
        {
            opts.runtime_filter_wait_timeout_ms = Some(i32::try_from(ms).unwrap_or(i32::MAX));
        }
    }
    query_options
}

pub(crate) fn build_runtime_state(
    inputs: RuntimeStateInputs,
    profiler: Option<&Profiler>,
) -> Result<Arc<RuntimeState>, String> {
    let cache_options = crate::runtime::cache::ExecutionCacheOptions::from_query_options(
        inputs.query_options.as_ref(),
    )?;
    let spill_config = inputs
        .query_options
        .as_ref()
        .and_then(|opts| opts.spill.clone());
    let spill_manager = if let Some(config) = spill_config.as_ref() {
        let runtime = inputs
            .execution_runtime
            .as_ref()
            .ok_or_else(|| "spill-enabled fragment requires an execution runtime".to_string())?;
        Some(Arc::new(QuerySpillManager::new(
            config.clone(),
            profiler,
            runtime,
        )))
    } else {
        None
    };
    Ok(Arc::new(
        RuntimeState::new(
            inputs.query_options,
            Some(cache_options),
            inputs.query_id,
            inputs.fragment_instance_id,
            inputs.backend_num,
            inputs.mem_tracker,
            spill_config,
            spill_manager,
            inputs.execution_runtime,
            inputs.scan_registration,
        )
        .with_runtime_filter_session(inputs.runtime_filter_session)
        .with_connector_staged_report_collector(inputs.connector_staged_report_collector),
    ))
}
