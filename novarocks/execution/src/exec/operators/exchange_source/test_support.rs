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
//! Explicit runtime configuration for operator receive-wait regressions.
use crate::runtime::execution_runtime::{
    ExchangeFlowLimits, ExecutionRuntime, test_execution_runtime,
};
use std::{sync::Arc, time::Duration};

pub(crate) fn runtime_with_transport_timeout(timeout: Duration) -> Arc<ExecutionRuntime> {
    let base = test_execution_runtime();
    let limits = base.exchange_flow_limits();
    Arc::new(
        ExecutionRuntime::new_with_exchange_flow_limits(
            base.config().clone(),
            base.function_set().clone(),
            base.memory_authority().clone(),
            ExchangeFlowLimits::try_new(
                limits.instance_floor_slots(),
                limits.instance_max_slots(),
                limits.floating_slots(),
                limits.stream_window(),
                timeout,
            )
            .unwrap(),
        )
        .unwrap(),
    )
}
