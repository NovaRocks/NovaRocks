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

//! Backend rendering of already-decided Worker task lifecycle facts.

//! This adapter owns no lifecycle state. It turns the Worker owner's exact
//! decisions into Backend observability and result-buffer cleanup.

use std::sync::Arc;

use novarocks_execution_contract::task_execution::identity::TaskIdentity;
use novarocks_worker::{
    TaskExecutionMetrics, TaskExecutionPorts, TaskProtocolEvent, TaskProtocolObserver,
    TaskResultLifecycle,
};

#[derive(Debug)]
struct BackendTaskExecutionPorts;

impl TaskProtocolObserver for BackendTaskExecutionPorts {
    fn observe(&self, event: TaskProtocolEvent) {
        super::marker::emit(event);
    }
}

impl TaskResultLifecycle for BackendTaskExecutionPorts {
    fn discard_task(&self, identity: TaskIdentity) {
        crate::runtime::result_buffer::discard_task(identity);
    }

    fn retire_task_result(&self, identity: TaskIdentity) {
        crate::runtime::result_buffer::retire_task_result(identity);
    }
}

impl TaskExecutionMetrics for BackendTaskExecutionPorts {
    fn record_task_created(&self) {
        novarocks_native_adapter::backend_metrics::record_task_execution_task_created();
    }
}

pub(crate) fn backend_task_execution_ports() -> TaskExecutionPorts {
    let adapter = Arc::new(BackendTaskExecutionPorts);
    let observer: Arc<dyn TaskProtocolObserver> = adapter.clone();
    let result_lifecycle: Arc<dyn TaskResultLifecycle> = adapter.clone();
    let metrics: Arc<dyn TaskExecutionMetrics> = adapter;
    TaskExecutionPorts::new(observer, result_lifecycle, metrics)
}
