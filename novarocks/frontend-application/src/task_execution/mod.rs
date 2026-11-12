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
//! Coordination Contract owns the facts actually shared with Query Application.
//! This module owns Task-local classifications and the frontend runtime owners: the stage and task
//! graph derived from the frozen static schedule, one `RemoteTask` per task,
//! one `StageExecution` per stage, one `ParticipationOwner` per query context,
//! a bounded and fair per-backend dispatcher, and a status intake with a
//! single serial runner.
//!
//! It produces immutable operation intents and consumes acknowledgements. It
//! contains no transport: [`intent::TaskOperationSink`] is the seam a later
//! transport owner implements, which is also what makes every behaviour here
//! testable without a wire.
//!
// Design: ADR-0161 (docs/adr/ADR-0161-pure-local-programs-and-supervised-task-lifecycle.md)

pub(crate) mod abort_effect;
#[cfg(test)]
pub(crate) mod actor_gate;
#[cfg(test)]
pub(crate) mod attempt_convergence;
#[cfg(test)]
pub(crate) mod attempt_pumps;
pub(crate) mod blocking_io;
pub(crate) mod clock;
pub(crate) mod completion;
#[cfg(test)]
pub(crate) mod completion_facts;
#[cfg(test)]
pub(crate) mod conclusion;
pub(crate) mod creation;
pub(crate) mod dispatch;
#[cfg(test)]
pub(crate) mod domain_tracker;
pub(crate) mod error;
#[cfg(test)]
pub(crate) mod execution;
#[cfg(test)]
pub(crate) mod feedback_pump;
pub(crate) mod graph;
pub(crate) mod intent;
pub(crate) mod lease;
pub(crate) mod manifest_projection;
#[cfg(test)]
pub(crate) mod manifest_round;
pub(crate) mod operation;
#[cfg(test)]
pub(crate) mod participation_convergence;
#[cfg(test)]
pub(crate) mod participation_owner;
#[cfg(test)]
pub(crate) mod participation_state;
pub(crate) mod profile_diagnostics;
pub(crate) mod protocol_facts;
pub(crate) mod pump_health;
#[cfg(test)]
pub(crate) mod remote_task;
#[cfg(test)]
pub(crate) mod round;
pub(crate) mod sources;
#[cfg(test)]
mod split_domain;
#[cfg(test)]
pub(crate) mod split_transport;
#[cfg(test)]
pub(crate) mod stage;
#[cfg(test)]
pub(crate) mod status_intake;
pub(crate) mod status_validation;

#[cfg(test)]
pub(crate) mod tests;

pub(crate) mod bridge;
pub(crate) mod host;
