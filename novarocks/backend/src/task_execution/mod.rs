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

//! The backend-local owner of the native task protocol.
//!
//! This module owns query context lifecycle, the task registry and its
//! creation transaction, per-task status, the observation channel, and
//! terminal retention. It owns no transport: every entry point takes a neutral
//! typed request from
//! `novarocks_execution::task_execution::operation` and returns a neutral
//! typed receipt, so it is fully drivable by an in-process caller and a
//! transport adapter adds only encoding.
//!
//! ```text
//! caller (transport adapter, or a test's fake caller)
//!   `- TaskExecutionRegistry ------ QueryContextHost   (shared facts)
//!        |                     `--- TaskExecutionHost  (receiver, capability, runnable)
//!        |
//!        +-- TaskStatusOwner  (one per task: the only status serializer)
//!        `-- TaskStatusSource (one per context: the observation channel)
//! ```
//!
//! [`ingress`] puts this owner behind the backend's RPC boundary, which makes
//! the protocol reachable over the wire. It routes no traffic: the
//! fragment-based lifecycle stack still owns every query, and binding this
//! owner to execution, then retiring the stack it replaces, are separate
//! steps.
// Design: ADR-0134 (docs/adr/ADR-0134-native-distributed-work-as-tasks.md)

mod clock;
mod context_host;
mod credential_slot;
mod domains;
mod entry;
mod execution_host;
mod fault;
mod feedback;
mod host;
pub(crate) mod ingress;
mod marker;
mod observation;
mod receipt;
mod registry;
pub(crate) mod shared_facts;
mod status;

#[cfg(test)]
mod tests;

pub use clock::{BackendMonotonicClock, ManualClock, ProcessMonotonicClock};
pub use context_host::NativeQueryContextHost;
pub use credential_slot::QueryContextCredentialSlot;
pub use execution_host::{
    InboundFrameAdmission, NativeRunnableTask, NativeTaskExecutionHost, TaskInboundCapabilities,
    TaskQueryContextFacts,
};
pub use host::{
    HostRejection, QueryContextHost, RunnableTask, SharedFactsRequest, TaskDynamicFilterRead,
    TaskExecutionHost,
};
pub(crate) use ingress::RegistryTaskExecutionIngress;
pub use observation::{
    CursorObservation, TaskStatusEvent, TaskStatusSource, TaskStatusSourceStats,
};
pub use receipt::{
    CancelTaskOutcome, CreateTaskOutcome, DynamicFilterReadOutcome, FinalTaskInfoOutcome,
    OperationReceipt, QueryContextOutcome, ReleaseAcknowledgement, ReleaseQueryContextOutcome,
    UpdateTaskOutcome,
};
pub use registry::{
    DeadlineSweep, RegistryCounters, TaskExecutionRegistry, TaskExecutionRegistryConfig,
};
pub use status::{
    METRIC_PUBLISH_MIN_INTERVAL, RootResultBinding, RootResultRoute, StatusAdvance,
    TaskMetricsSink, TaskStatusOwner, TaskStatusReporter,
};
