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

//! The frontend owners of the native task protocol.
//!
//! The neutral domain in `novarocks_execution::task_execution` owns the
//! values and every pure classification over them. This module owns the
//! frontend's runtime state machines built on top of it: the stage and task
//! graph derived from the frozen static schedule, one `RemoteTask` per task,
//! one `StageExecution` per stage, one `QueryContextOwner` per query context,
//! a bounded and fair per-backend dispatcher, and a status intake with a
//! single serial runner.
//!
//! It produces immutable operation intents and consumes acknowledgements. It
//! contains no transport: [`intent::TaskOperationSink`] is the seam a later
//! transport owner implements, which is also what makes every behaviour here
//! testable without a wire.
//!
//! Nothing in this module is routed into production yet. The existing
//! coordinator keeps owning distributed query execution untouched.
// Design: ADR-0134 (docs/adr/ADR-0134-native-distributed-work-as-tasks.md)

pub mod clock;
pub mod completion;
pub mod context_owner;
pub mod credential;
pub(crate) mod credential_pump;
pub mod dispatch;
pub mod error;
pub mod execution;
pub(crate) mod feedback_pump;
pub mod graph;
pub mod intent;
pub mod remote_task;
pub(crate) mod round;
pub mod sources;
mod split_domain;
pub(crate) mod split_transport;
pub mod stage;
pub mod status_intake;

#[cfg(test)]
mod tests;

pub use clock::{ManualClock, ProcessMonotonicClock, TaskProtocolClock};
pub use completion::{
    ReadCompletionTracker, ReadVerdict, WriteCompletionTracker, WriteVerdict, accept_final_info,
};
pub use context_owner::{
    ContextEstablishFacts, ContextEstablishSource, QueryContextOwner, ReleaseSettlement,
};
pub use credential::{CredentialRefreshOwner, RefreshRefusal, RefreshTiming, refresh_timing};
pub use dispatch::{ExpiredOperation, OperationDispatcher};
pub use error::{CapacityBound, TaskExecutionError};
pub use execution::{PumpReport, QueryTaskExecution, StatusReport};
pub use graph::{
    FragmentPlanFacts, FragmentPlanSource, TaskGraph, TaskGraphInputs, build_task_graph,
};
pub use intent::{
    AckPayload, DispatchBatch, OperationAcknowledgement, OperationIntent, TaskOperationSink,
};
pub use remote_task::{
    CreateSettlement, RemoteTask, RemoteTaskState, TaskTerminalReport, UpdateAdmission,
    UpdateSettlement,
};
// The coordinator still owns split delivery, so nothing consumes these yet.
// `expect` rather than `allow`, so the attribute itself stops compiling clean
// once the cutover gives them a caller.
#[expect(
    unused_imports,
    reason = "The split delivery bridge is wired by the coordinator cutover, which is a separate step."
)]
pub(crate) use split_transport::{
    DeliveryId, PendingSplitDelivery, SettleVerdict, SplitDeliveryBridge, SplitDeliveryError,
};
pub use stage::{EdgeOpenTracker, StageExecution};
pub use status_intake::{
    CountingWake, NotifyWake, StatusEvent, StatusIntake, StatusIntakeAdmission, StatusIntakeHandle,
    StatusIntakeWake,
};
