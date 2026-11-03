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

use crate::runtime::execution_runtime::ExecutionRuntime;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::query_options::QueryOptions;
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime_filter::RuntimeFilterSessionRef;
use novarocks_types::QueryId;
use novarocks_types::UniqueId;

pub(crate) struct RuntimeStateInputs {
    pub(crate) query_options: Option<QueryOptions>,
    pub(crate) query_id: Option<QueryId>,
    pub(crate) task_key: Option<UniqueId>,
    pub(crate) backend_num: Option<i32>,
    pub(crate) mem_tracker: Option<Arc<MemTracker>>,
    pub(crate) runtime_filter_session: Option<RuntimeFilterSessionRef>,
    pub(crate) execution_runtime: Option<Arc<ExecutionRuntime>>,
    pub(crate) query_memory: Option<crate::runtime::query_memory::QueryMemoryBinding>,
    pub(crate) task_identity: Option<novarocks_execution_contract::TaskIdentity>,
    pub(crate) task_control: Option<Arc<crate::runtime::task_control::TaskExecutionControl>>,
    #[cfg(novarocks_dev_controls)]
    pub(crate) exchange_test_observer: Option<Arc<dyn crate::runtime::exchange::TestEventObserver>>,
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
) -> super::ExecutionResult<Arc<RuntimeState>> {
    if let Some(memory) = inputs.query_memory.as_ref() {
        use crate::runtime::query_memory::QueryMemoryBindingError;
        if inputs.query_id != Some(memory.execution().query_id()) {
            return Err(QueryMemoryBindingError::QueryIdentityMismatch.into());
        }
        let identity = inputs
            .task_identity
            .ok_or(QueryMemoryBindingError::MissingTaskIdentity)?;
        let runtime = inputs
            .execution_runtime
            .as_ref()
            .ok_or(QueryMemoryBindingError::MissingExecutionRuntime)?;
        memory.validate_task(identity, runtime.memory_authority())?;
    }
    let error_state = match (inputs.task_identity, inputs.task_control) {
        (Some(identity), Some(control)) if identity == control.identity() => {
            crate::runtime::runtime_state::RuntimeErrorState::for_task(control)
        }
        (None, None) => crate::runtime::runtime_state::RuntimeErrorState::default(),
        (Some(_), None) => {
            return Err(super::ExecutionFailure::from_domain(
                crate::runtime::task_control::MissingTaskExecutionControl,
            ));
        }
        _ => {
            return Err(super::ExecutionFailure::from_domain(
                crate::runtime::task_control::TaskControlIdentityMismatch,
            ));
        }
    };
    let cache_options = crate::runtime::cache::ExecutionCacheOptions::from_query_options(
        inputs.query_options.as_ref(),
    )?;
    let state = RuntimeState::new(
        inputs.query_options,
        Some(cache_options),
        inputs.query_id,
        inputs.task_key,
        inputs.backend_num,
        inputs.mem_tracker,
        inputs.execution_runtime,
        Arc::new(error_state),
    )
    .with_runtime_filter_session(inputs.runtime_filter_session)
    .with_query_memory(inputs.query_memory);
    #[cfg(novarocks_dev_controls)]
    let state = state.with_exchange_test_observer(inputs.exchange_test_observer);
    Ok(Arc::new(state))
}
