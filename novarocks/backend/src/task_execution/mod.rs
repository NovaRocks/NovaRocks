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

//! Backend role adapters for the Worker-owned native task protocol.
//!
//! Worker owns query context lifecycle, the task registry and its creation
//! transaction, per-task status, the observation channel, and terminal
//! retention. This Backend module supplies role-local ports and execution hosts
//! to that owner; it owns no transport. Every entry point takes a neutral typed request from
//! `novarocks_execution_contract::task_execution::operation` and returns a neutral
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
// Design: ADR-0146 (docs/adr/ADR-0146-logical-execution-owns-attempts-and-result-visibility.md)

mod context_host;
mod execution_host;
pub(crate) mod ingress;

#[cfg(test)]
mod tests;

pub(crate) use context_host::NativeQueryContextHost;
pub(crate) use execution_host::{NativeTaskExecutionHost, TaskQueryContextFacts};
pub(crate) use ingress::RegistryTaskExecutionIngress;
