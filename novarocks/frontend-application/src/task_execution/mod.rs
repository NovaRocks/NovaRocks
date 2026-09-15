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
// Design: ADR-0146 (docs/adr/ADR-0146-logical-execution-owns-attempts-and-result-visibility.md)

pub(crate) mod abort_effect;
pub(crate) mod actor_gate;
pub(crate) mod blocking_io;
pub(crate) mod clock;
pub(crate) mod completion;
pub(crate) mod context_convergence;
pub(crate) mod context_owner;
pub(crate) mod credential;
pub(crate) mod credential_pump;
pub(crate) mod credential_residual_job;
pub(crate) mod dispatch;
pub(crate) mod error;
pub(crate) mod execution;
pub(crate) mod feedback_pump;
pub(crate) mod graph;
pub(crate) mod intent;
pub(crate) mod manifest_round;
pub(crate) mod remote_task;
pub(crate) mod round;
pub(crate) mod sources;
mod split_domain;
pub(crate) mod split_transport;
pub(crate) mod stage;
pub(crate) mod status_intake;

#[cfg(test)]
mod tests;
