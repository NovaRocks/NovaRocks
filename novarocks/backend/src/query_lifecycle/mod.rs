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

mod contract;
mod credential_lease;
mod entry;
mod local_runtime;
mod registry;
pub(crate) mod rpc;
pub(crate) mod stage;
pub(crate) mod task_update;

pub(crate) use contract::{
    BackendQueryControl, CatalogPruneOutcome, QueryControlAttachment, QueryHeartbeatDisposition,
    QueryLifecycleError, QueryLifecycleErrorCode, QueryLifecycleIngress,
    QueryTerminalFallbackTransport, QueryTerminalFallbackTransportError,
};
pub(crate) use local_runtime::NativeQueryLifecycleLocalRuntime;
pub use registry::QueryLifecycleRegistryConfig;
pub(crate) use registry::{
    QueryLifecycleRegistry, QueryLifecycleTerminalCleanup, StageBuildDecision,
};

#[cfg(test)]
mod tests;
